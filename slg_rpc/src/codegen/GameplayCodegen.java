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
 * 读取真实 protoc 输出的描述符，独立生成 GAME 分发和 Unity 调用代码。源码注释、方法换行、import 顺序等
 * 不影响类型解析。JDK 单文件模式允许本工具先于任何业务类编译运行。增加玩法只需修改 proto 和实现 handler。
 */
public final class GameplayCodegen {
    private static final String OPTIONS = "slg_gameplay_options.proto";

    private record MessageType(String javaName, String csName) {}
    private record Operation(int id, String method, MessageType request, MessageType response) {
        String constantName() { return method.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toUpperCase(Locale.ROOT); }
        String handlerName() { return Character.toLowerCase(method.charAt(0)) + method.substring(1); }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) throw new IllegalArgumentException("Expected descriptor-set, Java output and Unity output");
        FileDescriptorSet schema;
        try (var input = Files.newInputStream(Path.of(args[0]))) { schema = FileDescriptorSet.parseFrom(input); }
        List<Operation> operations = operations(schema);
        write(Path.of(args[1], "com/slg/rpc/generated/GameplayRoutes.java"), javaSource(operations));
        write(Path.of(args[2], "SlgGameplayApi.g.cs"), csSource(operations));
    }

    /** 自定义 option 的字段号也来自声明。生成器不包含 msg_id 扩展号或玩法编号的手工副本。 */
    private static int optionNumber(FileDescriptorSet schema) {
        for (FileDescriptorProto file : schema.getFileList()) {
            if (!OPTIONS.equals(file.getName())) continue;
            for (FieldDescriptorProto field : file.getExtensionList()) {
                if ("msg_id".equals(field.getName()) && ".google.protobuf.MethodOptions".equals(field.getExtendee())
                        && field.getType() == FieldDescriptorProto.Type.TYPE_UINT32) return field.getNumber();
            }
        }
        throw new IllegalArgumentException("Missing uint32 MethodOptions.msg_id in " + OPTIONS);
    }

    private static List<Operation> operations(FileDescriptorSet schema) {
        int option = optionNumber(schema);
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
                    operations.add(new Operation((int) id, method.getName(), request, response));
                }
            }
        }
        if (operations.isEmpty()) throw new IllegalArgumentException("No annotated gameplay operations");
        operations.sort(Comparator.comparingInt(Operation::id));
        return operations;
    }

    private static String javaSource(List<Operation> operations) {
        var out = new StringBuilder("""
                // Generated from gameplay method descriptors. Manual edits are overwritten.
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
            out.append("    public static final int ").append(op.constantName()).append(" = ").append(op.id).append(";\n");
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
                // Generated from gameplay method descriptors. Manual edits are overwritten.
                using System.Threading;
                using System.Threading.Tasks;

                namespace Slg.Unity
                {
                    public sealed partial class SlgTcpClient
                    {
                """);
        for (Operation op : operations) {
            out.append("        /// <summary>按协议选择消息编号与响应 parser，重试写命令时沿用原 requestId。</summary>\n")
                    .append("        public Task<").append(op.response.csName).append("> ").append(op.method)
                    .append("Async(string requestId, ").append(op.request.csName)
                    .append(" request, CancellationToken cancellation)\n        {\n")
                    .append("            return SendTypedAsync(requestId, ").append(op.id).append("u, request, ")
                    .append(op.response.csName).append(".Parser, cancellation);\n        }\n");
        }
        return out.append("    }\n}\n").toString();
    }

    private static void write(Path path, String source) throws IOException {
        Files.createDirectories(path.getParent());
        Files.writeString(path, source, StandardCharsets.UTF_8);
    }
}
