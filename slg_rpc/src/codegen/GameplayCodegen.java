import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 读取真实 protoc 输出的描述符，生成公共网络注册表、GAME 分发及 Unity 调用/推送分发代码。
 * 源码注释、方法换行、import 顺序不影响类型解析。编号、请求/响应/推送类型及处理域只有 proto 一处声明，
 * 避免 Unity 与 Java 各维护 switch 而发生串包；地图域不会混入 GAME 的领域处理器接口。
 * JDK 单文件模式允许本工具先于任何业务类编译运行，增加协议只需修改 proto 和实现对应 handler。
 */
public final class GameplayCodegen {
    private static final String OPTIONS = "slg_gameplay_options.proto";

    private record MessageType(String javaName, String csName) {}
    private record Operation(int id, String method, MessageType request, MessageType response, String domain, String origin) {
        String constantName() { return method.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toUpperCase(Locale.ROOT); }
        String handlerName() { return Character.toLowerCase(method.charAt(0)) + method.substring(1); }
    }

    private record Push(int id, MessageType type) {
        String constantName() { return constant(type.javaName.substring(type.javaName.lastIndexOf('.') + 1)); }
    }
    private static String constant(String name) {
        return name.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toUpperCase(Locale.ROOT);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) throw new IllegalArgumentException("Expected descriptor-set, Java output and Unity output");
        FileDescriptorSet schema;
        try (var input = Files.newInputStream(Path.of(args[0]))) { schema = FileDescriptorSet.parseFrom(input); }
        List<Operation> operations = operations(schema);
        List<Push> pushes = pushes(schema, operations);
        List<Operation> gameplay = operations.stream().filter(op -> op.domain.equals("GAME")).toList();
        write(Path.of(args[1], "com/slg/rpc/generated/GameplayRoutes.java"), javaSource(gameplay));
        write(Path.of(args[1], "com/slg/rpc/generated/WireRoutes.java"), wireJava(operations, pushes));
        write(Path.of(args[2], "SlgGameplayApi.g.cs"), csSource(operations));
        write(Path.of(args[2], "SlgWireRoutes.g.cs"), wireCs(operations, pushes));
    }

    /** 自定义 option 的字段号也来自声明。生成器不包含 msg_id 扩展号或玩法编号的手工副本。 */
    private static int optionNumber(FileDescriptorSet schema, String name, String extendee) {
        for (FileDescriptorProto file : schema.getFileList()) {
            if (!OPTIONS.equals(file.getName())) continue;
            for (FieldDescriptorProto field : file.getExtensionList()) {
                if (name.equals(field.getName()) && extendee.equals(field.getExtendee())) return field.getNumber();
            }
        }
        throw new IllegalArgumentException("Missing option " + name + " in " + OPTIONS);
    }

    private static List<Operation> operations(FileDescriptorSet schema) {
        int option = optionNumber(schema, "msg_id", ".google.protobuf.MethodOptions");
        int domainOption = optionNumber(schema, "route_domain", ".google.protobuf.MethodOptions");
        int originOption = optionNumber(schema, "request_origin", ".google.protobuf.MethodOptions");
        Map<String, MessageType> types = new HashMap<>();
        for (FileDescriptorProto file : schema.getFileList()) {
            // 玩法请求和响应使用顶层消息，Java 多文件与显式 C# 命名空间保证两端类型名直接可用。
            if (!file.getOptions().getJavaMultipleFiles() || !file.getOptions().hasCsharpNamespace()) continue;
            for (var msg : file.getMessageTypeList()) {
                String name = "." + file.getPackage() + "." + msg.getName();
                types.put(name, new MessageType(file.getOptions().getJavaPackage() + "." + msg.getName(),
                        "global::" + file.getOptions().getCsharpNamespace() + "." + msg.getName()));
            }
        }
        List<Operation> operations = new ArrayList<>();
        Set<Long> ids = new HashSet<>();
        Set<String> apiNames = new HashSet<>();
        Set<String> wireTypes = new HashSet<>();
        for (FileDescriptorProto file : schema.getFileList()) {
            if (!file.getDependencyList().contains(OPTIONS)) continue;
            for (ServiceDescriptorProto service : file.getServiceList()) {
                for (var method : service.getMethodList()) {
                    String location = service.getName() + "." + method.getName();
                    // MethodOptions 由通用描述符类解析，因此能按声明中的字段号取出未知扩展。
                    var values = method.getOptions().getUnknownFields().getField(option).getVarintList();
                    if (values.size() != 1 || values.getFirst() <= 0 || values.getFirst() > Integer.MAX_VALUE)
                        throw new IllegalArgumentException(location + " needs one msg_id in 1..2147483647");
                    long id = values.getFirst();
                    if (!ids.add(id)) throw new IllegalArgumentException(location + ": duplicate msg_id " + id);
                    if (!apiNames.add(method.getName().toLowerCase(Locale.ROOT)))
                        throw new IllegalArgumentException(location + ": duplicate generated API method name");
                    if (method.getClientStreaming() || method.getServerStreaming())
                        throw new IllegalArgumentException(location + ": gameplay calls require a single request and response");
                    if (!method.getName().matches("[A-Z][A-Za-z0-9]*"))
                        throw new IllegalArgumentException(location + ": expected a PascalCase operation name");
                    MessageType request = types.get(method.getInputType()), response = types.get(method.getOutputType());
                    if (request == null || response == null)
                        throw new IllegalArgumentException(location + ": use top-level messages with java_multiple_files and csharp_namespace");
                    // 客户端可以用请求类型反查唯一操作。快照等公共模型可以复用，顶层请求和响应必须各自独立。
                    if (!wireTypes.add(method.getInputType()) || !wireTypes.add(method.getOutputType()))
                        throw new IllegalArgumentException(location + ": each gameplay request and response requires a distinct message type");
                    String domain = enumOption(schema, "RouteDomain", method.getOptions().getUnknownFields(), domainOption);
                    String origin = enumOption(schema, "RequestOrigin", method.getOptions().getUnknownFields(), originOption);
                    if (!Set.of("GAME", "GATEWAY", "AOI").contains(domain)
                            || !Set.of("CLIENT", "SERVER", "BOTH").contains(origin))
                        throw new IllegalArgumentException(location + ": unknown route domain or request origin");
                    // GAME 只接受客户端命令；服务端请求只能由连接控制域发起，避免生成不可调度的入口。
                    if (!domain.equals("GATEWAY") && !origin.equals("CLIENT"))
                        throw new IllegalArgumentException(location + ": server initiated request requires GATEWAY domain");
                    operations.add(new Operation((int) id, method.getName(), request, response, domain, origin));
                }
            }
        }
        if (operations.isEmpty()) throw new IllegalArgumentException("No annotated gameplay operations");
        operations.sort(Comparator.comparingInt(Operation::id));
        return operations;
    }

    private static String javaSource(List<Operation> operations) {
        var out = new StringBuilder("""
                // Generated from protocol method descriptors. Manual edits are overwritten.
                package com.slg.rpc.generated;

                import com.google.protobuf.InvalidProtocolBufferException;
                import com.google.protobuf.Message;
                import com.slg.rpc.v1.PlayReply;
                import com.slg.rpc.v1.PlayRequest;
                import java.io.IOException;

                /** Gateway 用 supports 检查编号。GAME 将解码后的请求绑定到对应的领域方法。 */
                public final class GameplayRoutes {
                    private GameplayRoutes() {}

                """);
        // 需要特殊调度（如召回先锁矿点）的调用方使用同一份生成常量，避免出现手抄编号的旁路。
        for (Operation op : operations)
            out.append("    public static final int ").append(op.constantName()).append(" = WireRoutes.").append(op.constantName()).append(";\n");
        out.append("""

                    /** 领域处理器只接收经过对应 parser 解码的独立请求，返回声明中的精确类型。 */
                    public interface Handlers {
                """);
        for (Operation op : operations) {
            out.append("        ").append(op.response.javaName).append(' ').append(op.handlerName())
                    .append("(String requestId, ").append(op.request.javaName).append(" request) throws IOException;\n");
        }
        out.append("""
                    }

                    public static boolean supports(int msgId) {
                        return switch (msgId) {
                """);
        for (Operation op : operations) out.append("            case ").append(op.constantName()).append(" -> true;\n");
        out.append("""
                            default -> false;
                        };
                    }

                    /** GAME 可以先从已解码的请求解析需要独占的对象，再将同一请求提交给 handler。 */
                    public interface Invocation {
                        Message body();
                        PlayReply dispatch(Handlers handlers) throws IOException;
                    }

                    @FunctionalInterface private interface Action<T extends Message> {
                        Message handle(Handlers handlers, String id, T request) throws IOException;
                    }

                    private record Decoded<T extends Message>(int msgId, String requestId, T body,
                                                               Action<T> action) implements Invocation {
                        public PlayReply dispatch(Handlers handlers) throws IOException {
                            // 只在领域执行返回后确认成功。拒绝和提交结果不明由 GAME 分开处理。
                            Message response = action.handle(handlers, requestId, body);
                            return PlayReply.newBuilder().setMsgId(msgId).setRequestId(requestId)
                                    .setStatus(200).setBody(response.toByteString()).build();
                        }
                    }

                    public static Invocation decode(PlayRequest request) throws InvalidProtocolBufferException {
                        int msgId = request.getMsgId();
                        String id = request.getRequestId();
                        return switch (msgId) {
                """);
        for (Operation op : operations) {
            out.append("            case ").append(op.constantName()).append(" -> new Decoded<>(msgId, id,\n                    ")
                    .append(op.request.javaName).append(".parseFrom(request.getBody()), Handlers::")
                    .append(op.handlerName()).append(");\n");
        }
        return out.append("""
                            default -> throw new IllegalArgumentException("Unsupported gameplay msg_id " + msgId);
                        };
                    }
                }
                """).toString();
    }

    private static String csSource(List<Operation> operations) {
        var out = new StringBuilder("""
                // Generated from protocol method descriptors. Manual edits are overwritten.
                using System.Threading;
                using System.Threading.Tasks;

                namespace Slg.Unity
                {
                    public sealed partial class SlgTcpClient
                    {
                """);
        for (Operation op : operations) {
            if (op.origin.equals("SERVER")) continue;
            out.append("        /// <summary>按协议选择消息编号与响应 parser，重试写命令时沿用原 requestId。</summary>\n")
                    .append("        public Task<").append(op.response.csName).append("> ").append(op.method)
                    .append("Async(string requestId, ").append(op.request.csName)
                    .append(" request, CancellationToken cancellation)\n        {\n")
                    .append("            return SendTypedAsync(requestId, WireRoutes.").append(op.constantName()).append(", request, ")
                    .append(op.response.csName).append(".Parser, cancellation);\n        }\n");
        }
        return out.append("    }\n}\n").toString();
    }

    /** 按 proto 声明解析枚举扩展，不能把未知域悄悄退回 GAME 队列。 */
    private static String enumOption(FileDescriptorSet schema, String enumName,
                                     com.google.protobuf.UnknownFieldSet fields, int number) {
        var values = fields.getField(number).getVarintList();
        if (values.size() > 1) throw new IllegalArgumentException("Duplicate " + enumName + " option");
        long value = values.isEmpty() ? 0 : values.getFirst();
        for (var file : schema.getFileList()) if (file.getName().equals(OPTIONS))
            for (var type : file.getEnumTypeList()) if (type.getName().equals(enumName))
                for (var entry : type.getValueList()) if (entry.getNumber() == value) return entry.getName();
        throw new IllegalArgumentException("Unknown " + enumName + " value " + value);
    }

    private static List<Push> pushes(FileDescriptorSet schema, List<Operation> operations) {
        int option = optionNumber(schema, "push_id", ".google.protobuf.MessageOptions");
        Set<Integer> ids = new HashSet<>();
        Set<String> types = new HashSet<>(), constants = new HashSet<>();
        for (Operation op : operations) {
            ids.add(op.id); types.add(op.request.javaName); types.add(op.response.javaName);
            if (!constants.add(op.constantName())) throw new IllegalArgumentException("Duplicate generated constant name");
        }
        List<Push> pushes = new ArrayList<>();
        for (var file : schema.getFileList()) for (var message : file.getMessageTypeList()) {
            rejectNestedPush(message, option);
            var values = message.getOptions().getUnknownFields().getField(option).getVarintList();
            if (values.isEmpty()) continue;
            String location = file.getName() + ":" + message.getName();
            if (values.size() != 1 || values.getFirst() <= 0 || values.getFirst() > Integer.MAX_VALUE)
                throw new IllegalArgumentException(location + " needs one push_id in 1..2147483647");
            if (!file.getOptions().getJavaMultipleFiles() || !file.getOptions().hasCsharpNamespace())
                throw new IllegalArgumentException(location + ": use top-level messages with java_multiple_files and csharp_namespace");
            int id = values.getFirst().intValue();
            if (!ids.add(id)) throw new IllegalArgumentException(location + ": duplicate msg_id/push_id " + id);
            var type = new MessageType(file.getOptions().getJavaPackage() + "." + message.getName(),
                    "global::" + file.getOptions().getCsharpNamespace() + "." + message.getName());
            if (!types.add(type.javaName)) throw new IllegalArgumentException(location + ": each request, response and push requires a distinct message type");
            Push push = new Push(id, type);
            if (!constants.add(push.constantName())) throw new IllegalArgumentException(location + ": duplicate generated constant name");
            pushes.add(push);
        }
        pushes.sort(Comparator.comparingInt(Push::id));
        return pushes;
    }

    private static void rejectNestedPush(com.google.protobuf.DescriptorProtos.DescriptorProto message, int option) {
        for (var nested : message.getNestedTypeList()) {
            if (!nested.getOptions().getUnknownFields().getField(option).getVarintList().isEmpty())
                throw new IllegalArgumentException("push_id requires a top-level message type");
            rejectNestedPush(nested, option);
        }
    }

    /** 每个编号和类型由同一 descriptor 生成。Gateway 可仅读头选择域，正文仍按原字节转发。 */
    private static String wireJava(List<Operation> operations, List<Push> pushes) {
        var out = new StringBuilder("""
                // Generated from protocol descriptors. Manual edits are overwritten.
                package com.slg.rpc.generated;
                import com.google.protobuf.ByteString;
                import com.google.protobuf.InvalidProtocolBufferException;
                import com.google.protobuf.Message;
                import com.slg.rpc.WireFrame;
                public final class WireRoutes {
                    private WireRoutes() {}
                    public enum Domain { GAME, GATEWAY, AOI }
                """);
        for (var op : operations) out.append("    public static final int ").append(op.constantName()).append(" = ").append(op.id).append(";\n");
        for (var push : pushes) out.append("    public static final int ").append(push.constantName()).append(" = ").append(push.id).append(";\n");
        out.append("    public static Domain domain(int id) { return switch (id) {\n");
        for (var op : operations) out.append("        case ").append(op.constantName()).append(" -> Domain.").append(op.domain).append(";\n");
        out.append("        default -> throw new IllegalArgumentException(\"Unknown request msgId\"); }; }\n");
        for (String name : List.of("isRequest", "clientMayRequest", "serverMayRequest")) {
            out.append("    public static boolean ").append(name).append("(int id) { return switch (id) {\n");
            for (var op : operations) {
                boolean allowed = name.equals("isRequest") || name.equals("clientMayRequest") && !op.origin.equals("SERVER")
                        || name.equals("serverMayRequest") && !op.origin.equals("CLIENT");
                out.append("        case ").append(op.constantName()).append(" -> ").append(allowed).append(";\n");
            }
            out.append("        default -> false; }; }\n");
        }
        out.append("    public static boolean isPush(int id) { return switch (id) {\n");
        for (var push : pushes) out.append("        case ").append(push.constantName()).append(" -> true;\n");
        out.append("        default -> false; }; }\n");
        for (String side : List.of("Request", "Response", "Push")) {
            out.append("    public static Message parse").append(side).append("(int id, ByteString body) throws InvalidProtocolBufferException { return switch (id) {\n");
            if (side.equals("Push")) {
                for (var push : pushes) out.append("        case ").append(push.constantName()).append(" -> ").append(push.type.javaName).append(".parseFrom(body);\n");
            } else {
                for (var op : operations) out.append("        case ").append(op.constantName()).append(" -> ")
                        .append((side.equals("Request") ? op.request : op.response).javaName).append(".parseFrom(body);\n");
            }
            out.append("        default -> throw new IllegalArgumentException(\"Unknown ").append(side.toLowerCase(Locale.ROOT)).append(" msgId\"); }; }\n");
            out.append("    public static int ").append(side.toLowerCase(Locale.ROOT)).append("IdFor(Message body) {\n");
            if (side.equals("Push")) {
                for (var push : pushes) out.append("        if (body instanceof ").append(push.type.javaName).append(") return ").append(push.constantName()).append(";\n");
            } else {
                for (var op : operations) out.append("        if (body instanceof ").append((side.equals("Request") ? op.request : op.response).javaName)
                        .append(") return ").append(op.constantName()).append(";\n");
            }
            out.append("        throw new IllegalArgumentException(\"Unregistered ").append(side.toLowerCase(Locale.ROOT)).append(" type\"); }\n");
        }
        // 每种 push 生成精确类型的编码入口，业务调用方不需要传 msgId，也不能误填别的编号。
        for (var push : pushes) out.append("    public static WireFrame push(").append(push.type.javaName).append(" body) { return WireFrame.push(")
                .append(push.constantName()).append(", body); }\n");
        out.append("    public interface PushHandlers {\n");
        for (var push : pushes) out.append("        void on").append(push.type.javaName.substring(push.type.javaName.lastIndexOf('.') + 1))
                .append('(').append(push.type.javaName).append(" body);\n");
        out.append("    }\n    public static void dispatchPush(int id, ByteString body, PushHandlers handlers) throws InvalidProtocolBufferException { switch (id) {\n");
        for (var push : pushes) out.append("        case ").append(push.constantName()).append(" -> handlers.on")
                .append(push.type.javaName.substring(push.type.javaName.lastIndexOf('.') + 1)).append('(').append(push.type.javaName).append(".parseFrom(body));\n");
        return out.append("        default -> throw new IllegalArgumentException(\"Unknown push msgId\"); } }\n}\n").toString();
    }

    private static String csDomain(String domain) {
        return switch (domain) { case "GAME" -> "Game"; case "GATEWAY" -> "Gateway"; case "AOI" -> "Aoi";
            default -> throw new IllegalArgumentException("Unknown route domain"); };
    }

    private static String wireCs(List<Operation> operations, List<Push> pushes) {
        var out = new StringBuilder("""
                // Generated from protocol descriptors. Manual edits are overwritten.
                using System;
                using Google.Protobuf;
                namespace Slg.Unity
                {
                    public enum WireDomain { Game, Gateway, Aoi }
                    public static class WireRoutes
                    {
                """);
        for (var op : operations) out.append("        public const uint ").append(op.constantName()).append(" = ").append(op.id).append("u;\n");
        for (var push : pushes) out.append("        public const uint ").append(push.constantName()).append(" = ").append(push.id).append("u;\n");
        out.append("        public static WireDomain Domain(uint id) { switch (id) {\n");
        for (var op : operations) out.append("            case ").append(op.constantName()).append(": return WireDomain.").append(csDomain(op.domain)).append(";\n");
        out.append("            default: throw new ArgumentException(\"Unknown request msgId.\"); } }\n");
        for (String name : List.of("IsRequest", "ClientMayRequest", "ServerMayRequest")) {
            out.append("        public static bool ").append(name).append("(uint id) { switch (id) {\n");
            for (var op : operations) {
                boolean allowed = name.equals("IsRequest") || name.equals("ClientMayRequest") && !op.origin.equals("SERVER")
                        || name.equals("ServerMayRequest") && !op.origin.equals("CLIENT");
                out.append("            case ").append(op.constantName()).append(": return ").append(allowed).append(";\n");
            }
            out.append("            default: return false; } }\n");
        }
        out.append("        public static bool IsPush(uint id) { switch (id) {\n");
        for (var push : pushes) out.append("            case ").append(push.constantName()).append(": return true;\n");
        out.append("            default: return false; } }\n");
        for (String side : List.of("Request", "Response", "Push")) {
            out.append("        public static IMessage Parse").append(side).append("(uint id, ByteString body) { switch (id) {\n");
            if (side.equals("Push")) {
                for (var push : pushes) out.append("            case ").append(push.constantName()).append(": return ").append(push.type.csName).append(".Parser.ParseFrom(body);\n");
            } else {
                for (var op : operations) out.append("            case ").append(op.constantName()).append(": return ")
                        .append((side.equals("Request") ? op.request : op.response).csName).append(".Parser.ParseFrom(body);\n");
            }
            out.append("            default: throw new ArgumentException(\"Unknown ").append(side.toLowerCase(Locale.ROOT)).append(" msgId.\"); } }\n");
            out.append("        public static uint ").append(side).append("IdFor(IMessage body) {\n");
            if (side.equals("Push")) {
                for (var push : pushes) out.append("            if (body is ").append(push.type.csName).append(") return ").append(push.constantName()).append(";\n");
            } else {
                for (var op : operations) out.append("            if (body is ").append((side.equals("Request") ? op.request : op.response).csName)
                        .append(") return ").append(op.constantName()).append(";\n");
            }
            out.append("            throw new ArgumentException(\"Unregistered ").append(side.toLowerCase(Locale.ROOT)).append(" type.\"); }\n");
        }
        out.append("        public interface IPushHandlers {\n");
        for (var push : pushes) out.append("            void On").append(push.type.javaName.substring(push.type.javaName.lastIndexOf('.') + 1))
                .append('(').append(push.type.csName).append(" body);\n");
        out.append("        }\n        public static void DispatchPush(uint id, ByteString body, IPushHandlers handlers) { switch (id) {\n");
        for (var push : pushes) out.append("            case ").append(push.constantName()).append(": handlers.On")
                .append(push.type.javaName.substring(push.type.javaName.lastIndexOf('.') + 1)).append('(').append(push.type.csName).append(".Parser.ParseFrom(body)); return;\n");
        return out.append("            default: throw new ArgumentException(\"Unknown push msgId.\"); } }\n    }\n}\n").toString();
    }

    private static void write(Path path, String source) throws IOException {
        Files.createDirectories(path.getParent());
        Files.writeString(path, source, StandardCharsets.UTF_8);
    }
}
