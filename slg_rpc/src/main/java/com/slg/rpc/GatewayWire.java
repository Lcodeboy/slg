package com.slg.rpc;

/** Unity/Gateway 帧约定。两个上限均只计算 protobuf 消息，外层 4 字节长度头另计。 */
public final class GatewayWire {
    public static final int VERSION = 2;
    public static final int MAX_CLIENT_FRAME_BYTES = RpcTransport.MAX_REQUEST_BYTES;

    // GAME 可能返回恰好达到内部上限的 PlayReply。预留外层版本、字段标签和长度的开销，
    // 让边界上的合法回执仍能发送，同时让客户端在分配接收缓冲区前拒绝过大的长度声明。
    public static final int MAX_SERVER_FRAME_BYTES = RpcTransport.MAX_RESPONSE_BYTES + 16;

    private GatewayWire() {}
}
