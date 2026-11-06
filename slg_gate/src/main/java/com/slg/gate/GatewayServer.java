package com.slg.gate;

import com.slg.core.auth.TokenService;
import com.slg.core.http.HttpSupport;
import com.slg.core.http.RateLimiter;
import com.slg.rpc.GatewayWire;
import com.slg.rpc.v1.GatewayFrame;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.io.File;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import javax.net.ssl.SSLException;

/** 两个监听端口共享接入总额、Netty 线程和上游连接。每条客户端连接独享其会话及在途状态。 */
public final class GatewayServer implements AutoCloseable {
    private final NioEventLoopGroup boss = new NioEventLoopGroup(1);
    private final NioEventLoopGroup workers = new NioEventLoopGroup(Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors())));
    private final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);
    private final RateLimiter requests;
    private final RateLimiter authRequests;
    private final UpstreamClient upstream;
    private final GatewaySettings settings;
    private final TokenService tokens;
    private final AtomicInteger connections = new AtomicInteger();
    private final LongAdder rejectedConnections = new LongAdder();
    private final AtomicBoolean closed = new AtomicBoolean();
    private Channel httpListener, tcpListener;

    public GatewayServer(String uc, String game, TokenService tokens) {
        this(uc, game, tokens, GatewaySettings.defaults());
    }

    GatewayServer(String uc, String game, TokenService tokens, GatewaySettings settings) {
        this.settings = settings;
        this.requests = new RateLimiter(settings.ipRequestsPerMinute(), 60, Math.max(10_000, settings.maxConnections() * 2));
        this.authRequests = new RateLimiter(settings.authRequestsPerMinute(), 60, Math.max(10_000, settings.maxConnections() * 2));
        this.upstream = new UpstreamClient(uc, game, settings);
        this.tokens = tokens;
    }

    public void start(InetSocketAddress http, InetSocketAddress tcp) throws InterruptedException {
        start(http, tcp, tcpTls(System.getenv()));
    }

    /** 明文游戏流量只允许绑定回环。两次绑定作为一次启动，任一失败即释放所有端口和上游资源。 */
    synchronized void start(InetSocketAddress http, InetSocketAddress tcp, SslContext tls) throws InterruptedException {
        if (httpListener != null || closed.get()) throw new IllegalStateException("Gateway already started or closed");
        try {
            if (tls == null && (tcp.isUnresolved() || !tcp.getAddress().isLoopbackAddress()))
                throw new IllegalArgumentException("Non-loopback game TCP listener requires TLS certificate and key");
            // 最后开放 HTTP，使 /health 返回 UP 时对外游戏端口已经成功绑定。
            tcpListener = listen(tcp, pipeline -> {
                if (tls != null) pipeline.addLast(tls.newHandler(pipeline.channel().alloc()));
                // decoder 的长度上限含 4 字节头；超长只读头即拒绝，半包留待后续读取，粘包逐帧投递。
                pipeline.addLast(new LengthFieldBasedFrameDecoder(GatewayWire.MAX_CLIENT_FRAME_BYTES + 4, 0, 4, 0, 4));
                pipeline.addLast(new ProtobufDecoder(GatewayFrame.getDefaultInstance()));
                pipeline.addLast(new LengthFieldPrepender(4), new ProtobufEncoder());
                // 在拆帧之后计时，零星发送残缺字节不能无限续命。
                pipeline.addLast(new IdleStateHandler(60, 20, 0));
                pipeline.addLast(new GameTcpHandler(tokens, upstream, requests, authRequests, settings));
            });
            httpListener = listen(http, pipeline -> pipeline.addLast(new HttpServerCodec(4096, 8192, 8192),
                    new HttpObjectAggregator(HttpSupport.MAX_BODY_BYTES), new IdleStateHandler(60, 0, 0),
                    new GatewayHandler(tokens, upstream, requests, authRequests, this::health)));
        } catch (InterruptedException | RuntimeException | Error failure) {
            close();
            throw failure;
        }
    }

    private Channel listen(InetSocketAddress address, Consumer<ChannelPipeline> configure) throws InterruptedException {
        var bind = new ServerBootstrap().group(boss, workers).channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128).option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(32_768, 131_072))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel channel) {
                        if (!admit()) { channel.close(); return; }
                        channel.closeFuture().addListener(ignored -> connections.decrementAndGet());
                        channels.add(channel);
                        configure.accept(channel.pipeline());
                    }
                }).bind(address);
        // 绑定尚未完成时也加入关闭组，启动线程被中断时不会漏掉正在绑定的通道。
        channels.add(bind.channel());
        bind.await();
        if (!bind.isSuccess()) throw new IllegalStateException("Cannot bind gateway listener " + address, bind.cause());
        return bind.channel();
    }

    /** 账号 HTTP 的实际端口；测试传 0 时可在启动后读取分配结果。 */
    public int port() { return ((InetSocketAddress) httpListener.localAddress()).getPort(); }
    /** Unity 长连接的实际端口。 */
    public int tcpPort() { return ((InetSocketAddress) tcpListener.localAddress()).getPort(); }

    public void await() throws InterruptedException { httpListener.closeFuture().sync(); }

    private boolean admit() {
        for (;;) {
            int count = connections.get();
            if (closed.get() || count >= settings.maxConnections()) { rejectedConnections.increment(); return false; }
            if (connections.compareAndSet(count, count + 1)) return true;
        }
    }

    Map<String, Object> health() {
        return Map.of("status", closed.get() ? "STOPPING" : "UP", "service", "gate",
                "connections", connections.get(), "connectionLimit", settings.maxConnections(),
                "rejectedConnections", rejectedConnections.sum(), "rpc", upstream.metrics());
    }

    /** ChannelGroup 先关监听通道再关已接入连接。之后到达的注册也由 stayClosed 回收。 */
    @Override public synchronized void close() {
        if (!closed.compareAndSet(false, true)) return;
        channels.close().awaitUninterruptibly();
        upstream.close();
        boss.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
        workers.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
    }

    /** 公网游戏端口只使用网关自己的服务端证书。半套凭证或无效 PEM 使启动失败。 */
    static SslContext tcpTls(Map<String, String> env) {
        String cert = env.getOrDefault("SLG_GATE_TCP_TLS_CERT", "").trim();
        String key = env.getOrDefault("SLG_GATE_TCP_TLS_KEY", "").trim();
        if (cert.isEmpty() && key.isEmpty()) return null;
        if (cert.isEmpty() || key.isEmpty()) throw new IllegalArgumentException("Game TCP TLS certificate and key must be supplied together");
        try {
            return SslContextBuilder.forServer(new File(cert), new File(key)).sslProvider(SslProvider.JDK)
                    .protocols("TLSv1.3", "TLSv1.2").build();
        } catch (SSLException failure) {
            throw new IllegalArgumentException("Cannot configure game TCP TLS", failure);
        }
    }

    public static void main(String[] args) throws Exception {
        TokenService tokens = new TokenService(System.getenv("SLG_TOKEN_SECRET"), Clock.systemUTC());
        String bind = HttpSupport.env("SLG_BIND", "127.0.0.1");
        try (GatewayServer server = new GatewayServer(HttpSupport.env("SLG_UC_RPC_TARGET", "127.0.0.1:9081"),
                HttpSupport.env("SLG_GAME_RPC_TARGET", "127.0.0.1:9082"), tokens, GatewaySettings.fromEnvironment())) {
            Runtime.getRuntime().addShutdownHook(new Thread(server::close, "gateway-shutdown"));
            server.start(new InetSocketAddress(bind, HttpSupport.port("SLG_GATE_PORT", 8080)),
                    new InetSocketAddress(HttpSupport.env("SLG_GATE_TCP_HOST", bind), HttpSupport.port("SLG_GATE_TCP_PORT", 8090)));
            System.out.println("Gateway HTTP port " + server.port() + ", game TCP port " + server.tcpPort());
            server.await();
        }
    }
}
