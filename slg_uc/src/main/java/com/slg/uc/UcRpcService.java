package com.slg.uc;

import com.slg.core.auth.TokenService;
import com.slg.core.http.RateLimiter;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.v1.*;
import io.grpc.*;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

/** 有界账号认证服务：传输线程只校验、限流和入队，密码派生和数据库访问始终在认证线程。 */
final class UcRpcService extends AccountRpcGrpc.AccountRpcImplBase implements AutoCloseable {
    private static final Context.Key<String> REMOTE=Context.key("uc-remote-address");
    private final AccountRepository accounts;
    private final TokenService tokens;
    private final UcServer.Settings settings;
    private final ThreadPoolExecutor executor;
    private final Semaphore passwordWork;
    private final RateLimiter authRate=new RateLimiter(1200,60,10_000);
    private final RateLimiter accountRate=new RateLimiter(20,60,10_000);
    private final byte[] dummySalt=PasswordHasher.salt();
    private final LongAdder rejected=new LongAdder();

    UcRpcService(AccountRepository accounts,TokenService tokens,UcServer.Settings settings) {
        this.accounts=accounts;this.tokens=tokens;this.settings=settings;
        passwordWork=new Semaphore(settings.passwordConcurrency());
        executor=new ThreadPoolExecutor(settings.workers(),settings.workers(),0,TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(settings.queueCapacity()),Thread.ofPlatform().daemon().name("uc-auth-",0).factory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    /** 来源使用传输连接属性，不接受调用方自报的 IP。 */
    static ServerInterceptor peerInterceptor() {
        return new ServerInterceptor() {
            @Override public <ReqT,RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT,RespT> call,
                    Metadata headers,ServerCallHandler<ReqT,RespT> next) {
                var address=call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                String remote=address instanceof InetSocketAddress socket ? socket.getAddress().getHostAddress() : "unknown";
                return Contexts.interceptCall(Context.current().withValue(REMOTE,remote),call,headers,next);
            }
        };
    }

    @Override public void register(AuthRequest request,StreamObserver<AuthReply> observer) {authenticate(request,observer,true);}
    @Override public void login(AuthRequest request,StreamObserver<AuthReply> observer) {authenticate(request,observer,false);}

    @Override public void listRealms(EmptyRequest request, StreamObserver<RealmsReply> observer) {
        Context context = Context.current();
        try {
            tokens.verify(RpcMetadata.TOKEN.get());
        } catch (IllegalArgumentException unauthorized) {
            reply(context, observer, RealmsReply.newBuilder().setStatus(401)
                    .setError(error("UNAUTHORIZED", "Valid session required")).build());
            return;
        }
        var realm = RealmInfo.newBuilder().setRealmId(1).setName("冰原一区").setStatus("OPEN");
        reply(context, observer, RealmsReply.newBuilder().setStatus(200)
                .setDirectory(RealmDirectory.newBuilder().addRealms(realm)).build());
    }

    private void authenticate(AuthRequest request,StreamObserver<AuthReply> observer,boolean register) {
        Context context=Context.current();
        if(observer instanceof ServerCallStreamObserver<?> server) server.setOnCancelHandler(()->{});
        String username=request.getUsername().toLowerCase(Locale.ROOT),password=request.getPassword();
        if(!username.matches("[a-z0-9_]{3,32}") || password.length()<8 || password.length()>128) {
            reply(context,observer,failure(400,"INVALID_REQUEST","Use username (3-32 letters, digits or underscore) and password (8-128 characters)"));return;
        }
        if(!authRate.allow(Objects.requireNonNullElse(REMOTE.get(),"unknown")) || !accountRate.allow(username)) {
            reply(context,observer,failure(429,"RATE_LIMITED","Too many authentication attempts"));return;
        }
        long remaining=TimeUnit.MILLISECONDS.toNanos(settings.deadlineMillis());
        if(context.getDeadline()!=null) remaining=Math.min(remaining,context.getDeadline().timeRemaining(TimeUnit.NANOSECONDS));
        if(context.isCancelled() || remaining<=0) {
            reply(context,observer,failure(504,"DEADLINE_EXCEEDED","Authentication expired before execution"));return;
        }
        long deadline=System.nanoTime()+remaining;
        AuthTask task=new AuthTask(context,observer,()->{
            if(context.isCancelled() || System.nanoTime()-deadline>=0)
                return failure(504,"DEADLINE_EXCEEDED","Authentication expired before execution");
            if(!passwordWork.tryAcquire()) {
                rejected.increment();return failure(503,"BUSY","Authentication temporarily busy; request was not executed");
            }
            try {return issueSession(register,username,password);}
            catch(SQLException e) {return failure(503,"STORAGE_UNAVAILABLE","Account operation was not confirmed; try login before repeating registration");}
            finally {passwordWork.release();}
        });
        context.addListener(task.cancelled,Runnable::run);
        if(task.isCancelled()) return;
        try {executor.execute(task);}
        catch(RejectedExecutionException e) {
            rejected.increment();task.cancel(false);
            reply(context,observer,failure(503,"BUSY","Authentication queue is full or stopping; request was not accepted"));
        }
    }

    /** 注册确认丢失后应改走登录；本接口不把重复用户名当作幂等注册成功。 */
    private AuthReply issueSession(boolean register,String username,String password) throws SQLException {
        long id;
        if(register) {
            try {id=accounts.create(username,password);}
            catch(SQLException e) {
                if(e.getSQLState()!=null && e.getSQLState().startsWith("23"))
                    return failure(409,"USERNAME_TAKEN","Username already exists");
                throw e;
            }
        } else {
            var account=accounts.find(username);
            byte[] hash=PasswordHasher.hash(password,account.map(AccountRepository.Account::salt).orElse(dummySalt),
                    account.map(AccountRepository.Account::iterations).orElse(PasswordHasher.ITERATIONS));
            boolean matches=MessageDigest.isEqual(hash,account.map(AccountRepository.Account::hash).orElse(new byte[32]));
            if(account.isEmpty() || !matches) return failure(401,"INVALID_CREDENTIALS","Invalid username or password");
            id=account.get().accountId();
        }
        // JSON 与签名令牌统一使用十进制字符串，浏览器解析超过 2^53 的账号 ID 时仍能逐位相等。
        String accountId=Long.toString(id);
        String token=tokens.issue(accountId,Duration.ofHours(12));
        var session = LoginSession.newBuilder().setAccountId(accountId).setToken(token)
                .setExpiresAt(tokens.verify(token).expiresAt());
        return AuthReply.newBuilder().setStatus(register ? 201 : 200).setSession(session).build();
    }

    private final class AuthTask extends FutureTask<AuthReply> {
        private final Context context;
        private final StreamObserver<AuthReply> observer;
        private final Context.CancellationListener cancelled=ignored->{cancel(false);executor.remove(this);};
        AuthTask(Context context,StreamObserver<AuthReply> observer,Callable<AuthReply> work) {
            super(work);this.context=context;this.observer=observer;
        }
        @Override protected void done() {
            context.removeListener(cancelled);
            if(isCancelled() || context.isCancelled()) return;
            AuthReply result;
            try {result=get();}
            catch(InterruptedException e) {Thread.currentThread().interrupt();result=failure(503,"RESULT_UNKNOWN","Authentication result was not confirmed");}
            catch(ExecutionException e) {result=failure(503,"RESULT_UNKNOWN","Authentication result was not confirmed; try login before repeating registration");}
            reply(context,observer,result);
        }
    }

    Map<String,Object> metrics() {
        return Map.of("workers",settings.workers(),"active",executor.getActiveCount(),"queued",executor.getQueue().size(),
                "queueCapacity",settings.queueCapacity(),"rejected",rejected.sum(),
                "passwordActive",settings.passwordConcurrency()-passwordWork.availablePermits());
    }
    private static AuthReply failure(int status,String code,String message) {
        return AuthReply.newBuilder().setStatus(status).setError(error(code, message)).build();
    }
    private static BusinessError error(String code, String message) {
        return BusinessError.newBuilder().setCode(code).setMessage(message).build();
    }
    private static <T> void reply(Context context, StreamObserver<T> observer, T response) {
        if(context.isCancelled()) return;
        try {observer.onNext(response);observer.onCompleted();}
        catch(RuntimeException ignored) { /* 账号写入可能已经完成，断连不会触发自动重放。 */ }
    }
    /** 停止排队认证并等待正在执行的密码/数据库工作，再由拥有者关闭仓储。 */
    @Override public void close() {
        executor.shutdown();
        for(Runnable queued:executor.getQueue()) if(executor.remove(queued) && queued instanceof Future<?> future) future.cancel(false);
        boolean interrupted=false;
        while(!executor.isTerminated()) {
            try {executor.awaitTermination(1,TimeUnit.SECONDS);}
            catch(InterruptedException e) {interrupted=true;}
        }
        if(interrupted) Thread.currentThread().interrupt();
    }
}
