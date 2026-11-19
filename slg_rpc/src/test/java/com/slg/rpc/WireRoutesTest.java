package com.slg.rpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.UnknownFieldSet;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.generated.WireRoutes;
import com.slg.rpc.v1.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class WireRoutesTest {
    @Test void commonRegistryOwnsTypesDirectionsAndDomainsWithoutAddingMapCommandsToGameHandlers() throws Exception {
        assertEquals(WireRoutes.Domain.GATEWAY, WireRoutes.domain(WireRoutes.AUTH));
        assertEquals(WireRoutes.Domain.AOI, WireRoutes.domain(WireRoutes.MAP_WATCH));
        assertEquals(WireRoutes.Domain.GAME, WireRoutes.domain(WireRoutes.START_MARCH));
        assertFalse(GameplayRoutes.supports(WireRoutes.MAP_QUERY));
        assertFalse(GameplayRoutes.supports(WireRoutes.AUTH));
        assertTrue(WireRoutes.clientMayRequest(WireRoutes.AUTH));
        assertFalse(WireRoutes.serverMayRequest(WireRoutes.AUTH));
        assertTrue(WireRoutes.clientMayRequest(WireRoutes.HEARTBEAT));
        assertTrue(WireRoutes.serverMayRequest(WireRoutes.HEARTBEAT));
        assertFalse(WireRoutes.isRequest(WireRoutes.MAP_VIEW_UPDATE));
        assertTrue(WireRoutes.isPush(WireRoutes.MAP_VIEW_UPDATE));
        assertFalse(WireRoutes.clientMayRequest(WireRoutes.MAP_VIEW_UPDATE));
        assertInstanceOf(MapQueryRequest.class, WireRoutes.parseRequest(WireRoutes.MAP_QUERY, ByteString.EMPTY));
        assertInstanceOf(MapQueryReply.class, WireRoutes.parseResponse(WireRoutes.MAP_QUERY, ByteString.EMPTY));
        assertThrows(IllegalArgumentException.class, () -> WireRoutes.parsePush(WireRoutes.MAP_QUERY, ByteString.EMPTY));
        assertThrows(IllegalArgumentException.class, () -> WireRoutes.domain(0));
        assertNull(SlgGatewayProtocol.getDescriptor().findMessageTypeByName("GatewayFrame"));
        assertNull(MapQueryRequest.getDescriptor().findFieldByName("request_id"));
        assertNull(MapQueryReply.getDescriptor().findFieldByName("error"));
    }

    @Test void aTypedPushEncoderAndDispatcherUseTheSameDeclaredMessageId() throws Exception {
        var update = MapViewUpdate.newBuilder().setSubscriptionId("view-77").setThroughSeq(99).build();
        WireFrame push = WireRoutes.push(update);
        assertEquals(WireRoutes.MAP_VIEW_UPDATE, WireRoutes.pushIdFor(update));
        var seen = new AtomicReference<MapViewUpdate>();
        WireRoutes.dispatchPush(push.msgId(), push.body(), new WireRoutes.PushHandlers() {
            @Override public void onGatewayConnectionError(GatewayConnectionError body) { fail("Wrong push route"); }
            @Override public void onMapViewUpdate(MapViewUpdate body) { seen.set(body); }
        });
        assertEquals(update, seen.get());
        assertThrows(IllegalArgumentException.class, () -> WireRoutes.pushIdFor(StartMarchReq.getDefaultInstance()));
        assertThrows(IllegalArgumentException.class, () -> WireRoutes.requestIdFor(update));
    }

    @Test void addingPushOnlyChangesItsDeclarationAndDuplicateMethodOrPushIdsFailGeneration(@TempDir Path dir) throws Exception {
        var schema = schema();
        var file = schema.getFileBuilderList().stream().filter(f -> f.getName().equals("slg_map.proto")).findFirst().orElseThrow();
        var extra = file.addMessageTypeBuilder().setName("MailAdded");
        extra.getOptionsBuilder().setExtension(SlgGameplayOptions.pushId, 3001);
        assertEquals(0, runGenerator(schema, dir));
        String java = Files.readString(dir.resolve("com/slg/rpc/generated/WireRoutes.java"));
        String cs = Files.readString(dir.resolve("SlgWireRoutes.g.cs"));
        assertTrue(java.contains("MAIL_ADDED = 3001"));
        assertTrue(java.contains("push(com.slg.rpc.v1.MailAdded body)"));
        assertTrue(java.contains("handlers.onMailAdded(com.slg.rpc.v1.MailAdded.parseFrom(body))"));
        assertTrue(cs.contains("MAIL_ADDED = 3001u"));
        assertTrue(cs.contains("global::Slg.Rpc.V1.MailAdded.Parser.ParseFrom(body)"));
        extra.getOptionsBuilder().setExtension(SlgGameplayOptions.pushId, WireRoutes.START_MARCH);
        assertNotEquals(0, runGenerator(schema, dir));
        assertTrue(log(dir).contains("duplicate msg_id/push_id"));
        extra.getOptionsBuilder().setExtension(SlgGameplayOptions.pushId, WireRoutes.MAP_VIEW_UPDATE);
        assertNotEquals(0, runGenerator(schema, dir));
        assertTrue(log(dir).contains("duplicate msg_id/push_id"));
    }

    @Test void incompatibleDirectionUnknownDomainAndReusedPayloadTypesFailGeneration(@TempDir Path dir) throws Exception {
        var schema = schema();
        var file = schema.getFileBuilderList().stream().filter(f -> f.getName().equals("slg_march.proto")).findFirst().orElseThrow();
        var method = file.getServiceBuilder(0).getMethodBuilder(0);
        method.getOptionsBuilder().setExtension(SlgGameplayOptions.requestOrigin, SlgGameplayOptions.RequestOrigin.SERVER);
        assertNotEquals(0, runGenerator(schema, dir));
        assertTrue(log(dir).contains("server initiated request requires GATEWAY"));
        method.getOptionsBuilder().clearExtension(SlgGameplayOptions.requestOrigin);
        method.getOptionsBuilder().mergeUnknownFields(UnknownFieldSet.newBuilder().addField(SlgGameplayOptions.routeDomain.getNumber(),
                UnknownFieldSet.Field.newBuilder().addVarint(99).build()).build());
        assertNotEquals(0, runGenerator(schema, dir));
        assertTrue(log(dir).contains("Unknown RouteDomain"));
        schema = schema();
        file = schema.getFileBuilderList().stream().filter(f -> f.getName().equals("slg_march.proto")).findFirst().orElseThrow();
        var request = file.getMessageTypeBuilderList().stream().filter(m -> m.getName().equals("StartMarchReq")).findFirst().orElseThrow();
        request.getOptionsBuilder().setExtension(SlgGameplayOptions.pushId, 3901);
        assertNotEquals(0, runGenerator(schema, dir));
        assertTrue(log(dir).contains("distinct message type"));
    }

    private static FileDescriptorSet.Builder schema() throws Exception {
        return FileDescriptorSet.parseFrom(Files.readAllBytes(Path.of("target/generated-resources/protobuf/descriptor-sets/gameplay.pb"))).toBuilder();
    }
    private static String log(Path dir) throws Exception { return Files.readString(dir.resolve("generator.log")); }
    private static int runGenerator(FileDescriptorSet.Builder schema, Path dir) throws Exception {
        Path descriptor = dir.resolve("test.pb"); Files.write(descriptor, schema.build().toByteArray());
        Path protobufJar = Path.of(FileDescriptorSet.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--class-path", protobufJar.toString(), "src/codegen/GameplayCodegen.java", descriptor.toString(), dir.toString(), dir.toString())
                .redirectErrorStream(true).redirectOutput(dir.resolve("generator.log").toFile()).start();
        return process.waitFor();
    }
}
