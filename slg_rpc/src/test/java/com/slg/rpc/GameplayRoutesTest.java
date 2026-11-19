package com.slg.rpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.v1.HeroSelection;
import com.slg.rpc.v1.MarchView;
import com.slg.rpc.v1.PlayRequest;
import com.slg.rpc.v1.SlgGameplayOptions;
import com.slg.rpc.v1.SlgMarchProtocol;
import com.slg.rpc.v1.StartMarchReq;
import com.slg.rpc.v1.StartMarchRsp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.util.function.BiFunction;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class GameplayRoutesTest {
    private static final int START = SlgMarchProtocol.getDescriptor().findServiceByName("MarchCommands")
            .findMethodByName("StartMarch").getOptions().getExtension(SlgGameplayOptions.msgId);

    @Test void generatedRouteUsesDeclaredTypesAndPreservesFieldPresenceAndRequestId() throws Exception {
        // 这条黄金值固定已发布的线上操作编号，防止修改 proto 时误把旧客户端路由到另一种玩法。
        assertEquals(1001, START);
        // 0 是合法坐标。显式不带英雄与未提交英雄选择必须在解析后仍能区分。
        var data = StartMarchReq.newBuilder().setTargetX(0).setTargetY(150)
                .setHeroes(HeroSelection.getDefaultInstance()).build();
        var request = PlayRequest.newBuilder().setMsgId(START).setRequestId("army-17").setBody(data.toByteString()).build();
        assertTrue(GameplayRoutes.supports(START));
        var call = GameplayRoutes.decode(request);
        assertEquals(data, call.body());
        var reply = call.dispatch(onStartMarch((id, march) -> {
            assertEquals("army-17", id);
            assertSame(call.body(), march);
            assertTrue(march.hasTargetX());
            assertTrue(march.hasHeroes());
            return StartMarchRsp.newBuilder().setMarch(MarchView.newBuilder().setMarchId("m17")
                    .setArriveAt(9_007_199_254_740_993L)).build();
        }));
        assertEquals(START, reply.getMsgId());
        assertEquals(request.getRequestId(), reply.getRequestId());
        assertEquals(200, reply.getStatus());
        assertTrue(reply.hasBody());
        var parsed = StartMarchRsp.parseFrom(reply.getBody()).getMarch();
        assertEquals("m17", parsed.getMarchId());
        assertEquals(9_007_199_254_740_993L, parsed.getArriveAt());
    }

    @Test void invalidBodyAndUnknownIdCannotReachTheBusinessHandler() {
        var invoked = new AtomicBoolean();
        GameplayRoutes.Handlers handlers = onStartMarch((id, req) -> {
            invoked.set(true);
            return StartMarchRsp.getDefaultInstance();
        });
        var malformed = PlayRequest.newBuilder().setMsgId(START).setRequestId("r")
                .setBody(ByteString.copyFrom(new byte[] {(byte) 0x80})).build();
        assertThrows(InvalidProtocolBufferException.class, () -> GameplayRoutes.decode(malformed).dispatch(handlers));
        assertFalse(GameplayRoutes.supports(0));
        assertThrows(IllegalArgumentException.class,
                () -> GameplayRoutes.decode(malformed.toBuilder().setMsgId(0).build()).dispatch(handlers));
        assertFalse(invoked.get());
    }

    @Test void addingADescriptorMethodChangesBothLanguagesAndDuplicateOrMissingIdsFailTheBuild(@TempDir Path dir)
            throws Exception {
        var schema = FileDescriptorSet.parseFrom(Files.readAllBytes(
                Path.of("target/generated-resources/protobuf/descriptor-sets/gameplay.pb"))).toBuilder();
        var file = schema.getFileBuilderList().stream().filter(f -> f.getName().equals("slg_march.proto"))
                .findFirst().orElseThrow();
        var service = file.getServiceBuilder(0);
        // 从真实描述符派生一个全新的操作，在不改任何 Java/C# 模板的情况下生成两个入口。
        var sample = service.getMethod(0).toBuilder().setName("PreviewMarch");
        file.addMessageTypeBuilder().setName("PreviewMarchReq");
        file.addMessageTypeBuilder().setName("PreviewMarchRsp");
        sample.setInputType(".slg.rpc.v1.PreviewMarchReq").setOutputType(".slg.rpc.v1.PreviewMarchRsp");
        int newId = START + 10;
        setMsgId(sample, newId);
        var extra = service.addMethodBuilder().mergeFrom(sample.build());
        assertEquals(0, runGenerator(schema, dir));
        assertTrue(Files.readString(dir.resolve("com/slg/rpc/generated/GameplayRoutes.java"))
                .contains("Handlers::previewMarch)"));
        String unity = Files.readString(dir.resolve("SlgGameplayApi.g.cs"));
        assertTrue(unity.contains("PreviewMarchAsync("));
        assertTrue(unity.contains("SendTypedAsync(requestId, WireRoutes.PREVIEW_MARCH, request,"));
        assertTrue(Files.readString(dir.resolve("SlgWireRoutes.g.cs")).contains("PREVIEW_MARCH = " + newId + "u"));

        setMsgId(extra, START);
        assertNotEquals(0, runGenerator(schema, dir));
        assertTrue(Files.readString(dir.resolve("generator.log")).contains("duplicate msg_id"));
        extra.clearOptions();
        assertNotEquals(0, runGenerator(schema, dir));
        assertTrue(Files.readString(dir.resolve("generator.log")).contains("needs one msg_id"));
        setMsgId(extra, newId);
        extra.setInputType(service.getMethod(0).getInputType());
        assertNotEquals(0, runGenerator(schema, dir));
        assertTrue(Files.readString(dir.resolve("generator.log")).contains("distinct message type"));
    }

    /** 只为当前场景提供处理逻辑。若路由误投给另一玩法，测试立即失败。 */
    private static GameplayRoutes.Handlers onStartMarch(BiFunction<String, StartMarchReq, StartMarchRsp> handler) {
        return (GameplayRoutes.Handlers) Proxy.newProxyInstance(GameplayRoutes.Handlers.class.getClassLoader(),
                new Class<?>[]{GameplayRoutes.Handlers.class}, (proxy, method, args) -> {
                    assertEquals("startMarch", method.getName());
                    return handler.apply((String) args[0], (StartMarchReq) args[1]);
                });
    }

    private static void setMsgId(MethodDescriptorProto.Builder method, int id) {
        // 测试修改的描述符直接来自未注册扩展的二进制文件，替换未知字段中的旧值。
        method.getOptionsBuilder().clear().setExtension(SlgGameplayOptions.msgId, id);
    }

    private static int runGenerator(FileDescriptorSet.Builder schema, Path dir) throws Exception {
        Path descriptor = dir.resolve("test.pb");
        Files.write(descriptor, schema.build().toByteArray());
        Path protobufJar = Path.of(FileDescriptorSet.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--class-path", protobufJar.toString(), "src/codegen/GameplayCodegen.java",
                descriptor.toString(), dir.toString(), dir.toString())
                .redirectErrorStream(true).redirectOutput(dir.resolve("generator.log").toFile()).start();
        return process.waitFor();
    }
}
