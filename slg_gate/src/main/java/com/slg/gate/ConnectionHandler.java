package com.slg.gate;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.ScheduledFuture;
import java.net.InetSocketAddress;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/** HTTP 和 TCP 连接共用的生命周期。每条连接的字段只由自己所属的 EventLoop 修改。 */
abstract class ConnectionHandler<I> extends SimpleChannelInboundHandler<I> {
    protected boolean closing;
    private ScheduledFuture<?> slowClientDeadline;

    static String remote(ChannelHandlerContext ctx) {
        return ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress();
    }

    /** gRPC 线程只提交工作。连接断开、进入关闭流程或 EventLoop 已停止时放弃写回。 */
    final void dispatch(ChannelHandlerContext ctx, Runnable work) {
        try {
            ctx.executor().execute(() -> {
                if (!closing && ctx.channel().isActive()) work.run();
            });
        } catch (RejectedExecutionException ignored) { /* 线程组已退出，调用本身已释放许可。 */ }
    }

    @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof IdleStateEvent idle && idle.state() == IdleState.READER_IDLE) ctx.close();
        else super.userEventTriggered(ctx, event);
    }

    /** 高水位限制后续写入；若积压持续五秒，即使客户端不再发送请求也能回收连接。 */
    @Override public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isWritable()) {
            if (slowClientDeadline != null) { slowClientDeadline.cancel(false); slowClientDeadline = null; }
        } else if (slowClientDeadline == null) {
            slowClientDeadline = ctx.executor().schedule(() -> {
                if (!ctx.channel().isWritable()) ctx.close();
            }, 5, TimeUnit.SECONDS);
        }
        super.channelWritabilityChanged(ctx);
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        closing = true;
        if (slowClientDeadline != null) slowClientDeadline.cancel(false);
        super.channelInactive(ctx);
    }

    /** 输入解码失败后直接断开，避免在错误的帧边界上继续读取命令。 */
    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { ctx.close(); }
}
