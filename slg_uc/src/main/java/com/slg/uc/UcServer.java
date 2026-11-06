package com.slg.uc;

import com.slg.core.auth.TokenService;
import com.slg.core.http.HttpSupport;
import com.slg.uc.persistence.DatabaseEnvironment;
import com.slg.uc.persistence.PoolSettings;
import com.slg.core.http.HttpResult;
import com.slg.rpc.RpcTransport;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Server;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * UC 内部 gRPC 账号服务与独立 HTTP 健康探针。认证使用有界工作池，健康检查不访问数据库。
 * 服务拥有仓储；关闭先停止 RPC 并等待正在执行的认证，最后关闭账号连接池。
 */
public final class UcServer implements AutoCloseable {
    private final HttpServer health;
    private final ExecutorService healthExecutor;
    private final UcRpcService service;
    private final AccountRepository accounts;
    private final Server rpc;
    private final AtomicBoolean closed=new AtomicBoolean();

    /** 绑定失败或配置失败均释放本服务接管的仓储；start 前不会接收业务调用。 */
    public UcServer(InetSocketAddress address,InetSocketAddress rpcAddress,AccountRepository accounts,
                    TokenService tokens,Settings settings) throws IOException {
        this.accounts=Objects.requireNonNull(accounts);
        HttpServer createdHealth=null;
        ExecutorService createdExecutor=null;
        UcRpcService createdService=null;
        Server createdRpc=null;
        try {
            Objects.requireNonNull(tokens);Objects.requireNonNull(settings);
            createdHealth=HttpServer.create(address,128);
            createdExecutor=new ThreadPoolExecutor(2,2,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(64),
                    Thread.ofPlatform().daemon().name("uc-health-",0).factory(),new ThreadPoolExecutor.AbortPolicy());
            createdService=new UcRpcService(accounts,tokens,settings);
            createdRpc=RpcTransport.serverBuilder(rpcAddress).intercept(UcRpcService.peerInterceptor())
                    .addService(createdService).build();
            health=createdHealth;healthExecutor=createdExecutor;service=createdService;rpc=createdRpc;
            health.setExecutor(healthExecutor);health.createContext("/",this::handleHealth);
        } catch(IOException | RuntimeException | Error failure) {
            if(createdRpc!=null) createdRpc.shutdownNow();
            if(createdHealth!=null) createdHealth.stop(0);
            if(createdExecutor!=null) createdExecutor.shutdownNow();
            if(createdService!=null) createdService.close();
            accounts.close();throw failure;
        }
    }

    public void start() throws IOException {
        try {rpc.start();health.start();}
        catch(IOException | RuntimeException | Error failure) {close();throw failure;}
    }
    public int port() {return health.getAddress().getPort();}
    public int rpcPort() {return rpc.getPort();}

    /** HTTP 端口只支持健康检查，原注册、登录及区服 HTTP 路径均不再提供业务能力。 */
    private void handleHealth(HttpExchange exchange) throws IOException {
        try {
            if(exchange.getRequestURI().getRawQuery()!=null) {
                HttpSupport.json(exchange,400,HttpResult.fail("INVALID_REQUEST","Query parameters are not supported"));return;
            }
            if(!"/health".equals(exchange.getRequestURI().getPath())) {
                HttpSupport.json(exchange,404,HttpResult.fail("NOT_FOUND","Business requests require the internal RPC endpoint"));return;
            }
            if(!"GET".equals(exchange.getRequestMethod())) {
                HttpSupport.json(exchange,405,HttpResult.fail("METHOD_NOT_ALLOWED","Use GET"));return;
            }
            HttpSupport.json(exchange,200,Map.of("status","UP","service","uc","rpcPort",rpcPort(),
                    "transport","grpc","authentication",service.metrics()));
        } finally {exchange.close();}
    }

    @Override public void close() {
        if(!closed.compareAndSet(false,true)) return;
        rpc.shutdownNow();health.stop(0);healthExecutor.shutdownNow();
        try {service.close();}
        finally {accounts.close();}
        try {rpc.awaitTermination(5,TimeUnit.SECONDS);}
        catch(InterruptedException e) {Thread.currentThread().interrupt();}
    }

    /** 认证队列和密码计算均有独立上限，拒绝策略不会让传输线程代为计算密码。 */
    public record Settings(int workers,int queueCapacity,int passwordConcurrency,int deadlineMillis) {
        public Settings {
            if(workers<1 || workers>32 || queueCapacity<1 || queueCapacity>4096 || passwordConcurrency<1
                    || passwordConcurrency>workers || deadlineMillis<1 || deadlineMillis>30_000)
                throw new IllegalArgumentException("Invalid UC execution settings");
        }
        public static Settings defaults() {return new Settings(8,128,8,5000);}
    }

    /** MySQL 凭证按已有环境配置读取；两个监听地址分别用于健康探针和内部 RPC。 */
    public static void main(String[] args) throws Exception {
        System.setProperty("sun.net.httpserver.maxReqTime","30");
        System.setProperty("sun.net.httpserver.maxRspTime","30");
        Clock clock=Clock.systemUTC();
        TokenService tokens=new TokenService(System.getenv("SLG_TOKEN_SECRET"),clock);
        AccountRepository accounts=new AccountRepository(DatabaseEnvironment.jdbcUrl(),DatabaseEnvironment.user(),
                DatabaseEnvironment.password(),clock,PoolSettings.fromEnvironment());
        UcServer app=null;
        try {
            app=new UcServer(new InetSocketAddress(HttpSupport.env("SLG_BIND","127.0.0.1"),HttpSupport.port("SLG_UC_PORT",8081)),
                    new InetSocketAddress(HttpSupport.env("SLG_RPC_BIND",HttpSupport.env("SLG_BIND","127.0.0.1")),
                            HttpSupport.port("SLG_UC_RPC_PORT",9081)),accounts,tokens,Settings.defaults());
            UcServer running=app;
            Runtime.getRuntime().addShutdownHook(new Thread(running::close,"uc-shutdown"));
            app.start();System.out.println("UC health port "+app.port()+", RPC port "+app.rpcPort());
        } catch(Exception | Error failure) {if(app!=null) app.close();else accounts.close();throw failure;}
    }
}
