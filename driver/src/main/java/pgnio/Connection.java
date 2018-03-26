package pgnio;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Base class for all connections. Implementations are instances of either {@link Startup} or {@link Started} */
public abstract class Connection implements AutoCloseable {
  /** Logger, made visible for those that might want to call {@link Logger#addHandler(Handler)} */
  public static final Logger log = Logger.getLogger(Connection.class.getName());

  /** Make initial DB connection and return ready for startup */
  public static CompletableFuture<Startup> init(Config config) {
    log.log(Level.FINE, "Connecting to {0}", config.hostname);
    return config.ioConnector.apply(config).thenApply(io -> new Startup(config, io));
  }

  /** {@link #init(Config)} + {@link Startup#auth()} */
  public static CompletableFuture<QueryReadyConnection.AutoCommit> authed(Config config) {
    return init(config).thenCompose(Startup::auth);
  }

  protected final Context ctx;

  protected Connection(Context ctx) { this.ctx = ctx; }

  /** Calls {@link #terminate()} and waits for a response */
  @Override
  public void close() throws ExecutionException, InterruptedException { terminate().get(); }

  protected CompletableFuture<Void> sendTerminate() {
    ctx.buf.clear();
    ctx.writeByte((byte) 'X').writeLengthIntBegin().writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  /** Send terminate and close the connection */
  public CompletableFuture<Void> terminate() {
    return sendTerminate().handle((__, termEx) ->
      ctx.io.close().handle((___, closeEx) -> {
        Throwable ex = termEx != null ? termEx : closeEx;
        if (ex == null) return (Void) null;
        if (termEx != null && closeEx != null) log.log(Level.WARNING, "Failed closing", closeEx);
        if (ex instanceof RuntimeException) throw (RuntimeException) ex;
        throw new RuntimeException(ex);
      })
    ).thenCompose(Function.identity());
  }

  protected <@Nullable T> CompletableFuture<T> terminate(T ret, @Nullable Throwable ex) {
    return terminate().handle((__, termEx) -> {
      if (ex != null && termEx != null) log.log(Level.WARNING, "Failed terminating", termEx);
      Throwable rethrow = ex == null ? termEx : ex;
      if (rethrow != null) {
        if (ex instanceof RuntimeException) throw (RuntimeException) ex;
        throw new RuntimeException(ex);
      }
      return ret;
    });
  }

  /** Shortcut for {@link #terminate()} for use with {@link CompletableFuture#handle(BiFunction)} */
  public <T> CompletableFuture<T> terminated(@Nullable CompletableFuture<T> ret, @Nullable Throwable ex) {
    if (ret != null) return terminated(ret);
    return terminate(ret, ex).thenCompose(Function.identity());
  }

  /** Shortcut for {@link #terminate()} */
  public <T> CompletableFuture<T> terminated(CompletableFuture<T> fut) {
    return fut.handle(this::terminate).thenCompose(Function.identity());
  }

  /** Subscription management for notices on this connection */
  public Subscribable<Subscribable.Notice> notices() { return ctx.noticeSubscribable; }
  /**
   * Subscription management for notifications on this connection. Note, a LISTEN statement is still required in
   * addition to this call.
   */
  public Subscribable<Subscribable.Notification> notifications() { return ctx.notificationSubscribable; }
  /** Subscription management for parameter status changes on this connection */
  public Subscribable<Subscribable.ParameterStatus> parameterStatuses() { return ctx.parameterStatusSubscribable; }

  /** Read only map of runtime parameters sent from the connection on startup or changed during use */
  public Map<String, String> getRuntimeParameters() { return Collections.unmodifiableMap(ctx.runtimeParameters); }

  protected CompletableFuture<Void> readBackendMessage() {
    return readBackendMessage(ctx.config.defaultTimeout, ctx.config.defaultTimeoutUnit);
  }

  protected CompletableFuture<Void> readBackendMessage(long timeout, TimeUnit timeoutUnit) {
    ctx.buf.clear();
    // We need 5 bytes to get the type and size
    ctx.writeEnsureCapacity(5).limit(5);
    return ctx.io.readFull(ctx.buf, timeout, timeoutUnit).thenCompose(__ -> {
      // Now that we have the size, make sure we have enough capacity to fulfill it and reset the limit
      int messageSize = ctx.buf.getInt(1);
      if (log.isLoggable(Level.FINER))
        log.log(Level.FINER, "{0} Read message header of type {1} with size {2}",
            new Object[] { ctx, (char) ctx.buf.get(0), messageSize });
      ctx.writeEnsureCapacity(messageSize).limit(1 + messageSize);
      // Fill it with the rest of the message and flip it for use
      return ctx.io.readFull(ctx.buf, timeout, timeoutUnit).thenRun(() -> ctx.buf.flip());
    });
  }

  protected CompletableFuture<Void> writeFrontendMessage() {
    return writeFrontendMessage(ctx.config.defaultTimeout, ctx.config.defaultTimeoutUnit);
  }

  protected CompletableFuture<Void> writeFrontendMessage(long timeout, TimeUnit timeoutUnit) {
    if (log.isLoggable(Level.FINER))
      log.log(Level.FINER, "{0} Writing message with first char {1} and size {2}",
          new Object[] { ctx, (char) ctx.buf.get(0), ctx.buf.limit() });
    return ctx.io.writeFull(ctx.buf, timeout, timeoutUnit).thenRun(() -> ctx.buf.clear());
  }

  /**
   * Assuming buffer populated w/ message, this peeks and if it is a general message, it handles it and returns a
   * future. If it is not a general message, it returns null.
   */
  protected @Nullable CompletableFuture<Void> handleGeneralResponse() {
    char typ = (char) ctx.buf.get(0);
    switch (typ) {
      // Notification Response
      case 'A':
        ctx.buf.position(5);
        return notifications().publish(new Subscribable.Notification(ctx.buf.getInt(), ctx.bufReadString(), ctx.bufReadString()));
      // ErrorMessage
      case 'E':
      // NoticeResponse
      case 'N':
        // Throw error or handle notice after skipping the length
        ctx.buf.position(5);
        Map<Byte, String> fields = new HashMap<>();
        while (true) {
          byte b = ctx.buf.get();
          if (b == 0) break;
          fields.put(b, ctx.bufReadString());
        }
        Subscribable.Notice notice = new Subscribable.Notice(fields);
        // Throw on error, send to subscriber on normal notice
        if (typ == 'N') return notices().publish(notice);
        // Sometimes we ignore errors like during connection reset
        DriverException.FromServer ex = new DriverException.FromServer(notice);
        if (ctx.ignoreErrors) {
          log.log(Level.FINE, "Ignoring error", ex);
          return CompletableFuture.completedFuture(null);
        }
        throw ex;
      // ParameterStatus
      case 'S':
        // Handle status after skipping length
        ctx.buf.position(5);
        Subscribable.ParameterStatus status =
            new Subscribable.ParameterStatus(ctx.bufReadString(), ctx.bufReadString());
        ctx.runtimeParameters.put(status.parameter, status.value);
        return parameterStatuses().publish(status);
      default: return null;
    }
  }

  /** Basically {@link #readBackendMessage()} repeatedly until not handled with {@link #handleGeneralResponse()} */
  protected CompletableFuture<Void> readNonGeneralBackendMessage() {
    return readBackendMessage().thenCompose(__ -> {
      CompletableFuture<Void> generalHandled = handleGeneralResponse();
      if (generalHandled == null) return CompletableFuture.completedFuture(null);
      return generalHandled.thenCompose(___ -> readNonGeneralBackendMessage());
    });
  }

  /** Assuming buffer is at position to read status, this reads it and sets it */
  protected void updateReadyForQueryTransactionStatus() {
    char status = (char) ctx.buf.get();
    switch (status) {
      case 'I':
        ctx.lastTransactionStatus = QueryReadyConnection.TransactionStatus.IDLE;
        break;
      case 'T':
        ctx.lastTransactionStatus = QueryReadyConnection.TransactionStatus.IN_TRANSACTION;
        break;
      case 'E':
        ctx.lastTransactionStatus = QueryReadyConnection.TransactionStatus.FAILED_TRANSACTION;
        break;
      default: throw new IllegalArgumentException("Unrecognized transaction status: " + status);
    }
  }

  /** Contextual state kept by a connection */
  public static class Context extends BufWriter.Simple<Context> {
    /** The config */
    public final Config config;
    /** The IO connection. Usually immutable, but can be replaced when going to SSL */
    public ConnectionIo io;

    protected final Subscribable<Subscribable.Notice> noticeSubscribable = new Subscribable<>();
    protected final Subscribable<Subscribable.Notification> notificationSubscribable = new Subscribable<>();
    protected final Subscribable<Subscribable.ParameterStatus> parameterStatusSubscribable = new Subscribable<>();
    protected @Nullable Integer processId;
    protected @Nullable Integer secretKey;
    protected final Map<String, String> runtimeParameters = new HashMap<>();
    protected QueryReadyConnection.@Nullable TransactionStatus lastTransactionStatus;
    protected boolean ignoreErrors;

    @SuppressWarnings("initialization")
    public Context(Config config, ConnectionIo io) {
      super(config.directBuffer, config.bufferStep);
      this.config = config;
      this.io = io;
      // Add the notice log if we are logging em
      if (config.logNotices) noticeSubscribable.subscribe(n -> {
        n.log(log, this);
        return CompletableFuture.completedFuture(null);
      });
    }

    /** Read a null-terminated string off the buffer */
    public String bufReadString() {
      int indexOfZero = buf.position();
      while (buf.get(indexOfZero) != 0) indexOfZero++;
      // Temporarily put the limit for decoding
      int prevLimit = buf.limit();
      buf.limit(indexOfZero);
      String ret = Util.stringFromByteBuffer(buf);
      buf.limit(prevLimit);
      // Read the zero
      buf.position(buf.position() + 1);
      return ret;
    }

    @Override
    public String toString() {
      return "[" + config.username + "@" + config.hostname + ":" + config.port + "->" +
          io.getLocalPort() + "/" + config.database + "]";
    }
  }

  /** Connection connected but not authed */
  public static class Startup extends Connection {
    protected Startup(Config config, ConnectionIo io) { super(new Context(config, io)); }

    /** Perform the auth handshake */
    public CompletableFuture<QueryReadyConnection.AutoCommit> auth() {
      CompletableFuture<?> sslComplete;
      if (ctx.config.ssl == null || ctx.config.ssl) sslComplete = startSsl(ctx.config.ssl != null);
      else sslComplete = CompletableFuture.completedFuture(null);
      return sslComplete.thenCompose(__ -> doAuth());
    }

    protected CompletableFuture<Void> startSsl(boolean required) {
      log.log(Level.FINE, "{0} Starting SSL", ctx);
      // Send SSLRequest
      ctx.buf.clear();
      ctx.writeInt(8).writeInt(80877103);
      ctx.buf.flip();
      return writeFrontendMessage().thenCompose(__ -> {
        // Just a single byte of 'S' or 'N' (yes or no)
        ctx.buf.clear().limit(1);
        return ctx.io.readFull(ctx.buf, ctx.config.defaultTimeout, ctx.config.defaultTimeoutUnit).thenCompose(___ -> {
          char response = (char) ctx.buf.get(0);
          if (response == 'N') {
            if (required) throw new DriverException.ServerSslNotSupported();
            log.log(Level.INFO, "{0} SSL not supported by server, continuing unencrypted", ctx);
            return CompletableFuture.completedFuture(null);
          }
          if (response != 'S') throw new IllegalArgumentException("Unrecognized SSL response char: " + response);
          return ctx.config.sslWrapper.apply(ctx.io).thenApply(sslIo -> {
            ctx.io = sslIo;
            return null;
          });
        });
      });
    }

    protected CompletableFuture<QueryReadyConnection.AutoCommit> doAuth() {
      log.log(Level.FINE, "{0} Authenticating", ctx);
      // Send startup message
      ctx.buf.clear();
      ctx.writeLengthIntBegin().writeInt(ctx.config.protocolVersion).writeCString("user").
          writeCString(ctx.config.username);
      if (ctx.config.database != null) ctx.writeCString("database").writeCString(ctx.config.database);
      if (ctx.config.additionalStartupParams != null)
        ctx.config.additionalStartupParams.forEach((k, v) -> ctx.writeCString(k).writeCString(v));
      ctx.writeByte((byte) 0).writeLengthIntEnd();
      ctx.buf.flip();
      return writeFrontendMessage().thenCompose(__ -> readAuthResponse());
    }

    protected CompletableFuture<QueryReadyConnection.AutoCommit> readAuthResponse() {
      return readNonGeneralBackendMessage().thenCompose(__ -> {
        char typ = (char) ctx.buf.get();
        if (typ != 'R') throw new IllegalArgumentException("Unrecognized auth response message: " + typ);
        // Skip the length, grab the auth type
        ctx.buf.position(5);
        int authType = ctx.buf.getInt();
        if (log.isLoggable(Level.FINE))
          log.log(Level.FINE, "{0} Got auth response of type {1}", new Object[] { ctx, authType });
        switch (authType) {
          // AuthenticationOk
          case 0: return readPostAuthResponse();
          // AuthenticationCleartextPassword
          case 3: return sendClearTextPassword();
          // AuthenticationMD5Password
          case 5: return sendMd5Password(ctx.buf.get(), ctx.buf.get(), ctx.buf.get(), ctx.buf.get());
          // Other...
          default: throw new IllegalArgumentException("Unrecognized auth response type: " + authType);
        }
      });
    }

    protected CompletableFuture<QueryReadyConnection.AutoCommit> sendClearTextPassword() {
      if (ctx.config.password == null) throw new IllegalStateException("Password requested, none provided");
      ctx.buf.clear();
      ctx.writeByte((byte) 'p').writeLengthIntBegin().writeCString(ctx.config.password).writeLengthIntEnd();
      ctx.buf.flip();
      return writeFrontendMessage().thenCompose(__ -> readAuthResponse());
    }

    protected CompletableFuture<QueryReadyConnection.AutoCommit> sendMd5Password(byte... salt) {
      if (ctx.config.password == null) throw new IllegalStateException("Password requested, none provided");
      // "md5" + md5(md5(password + username) + random-salt))
      ctx.buf.clear();
      ctx.writeByte((byte) 'p').writeLengthIntBegin().
          writeByte((byte) 'm').writeByte((byte) 'd').writeByte((byte) '5');
      MessageDigest md5;
      try {
        md5 = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) { throw new RuntimeException(e); }
      byte[] hash = Util.md5Hex(md5,
          Util.md5Hex(md5, ctx.config.password.getBytes(StandardCharsets.UTF_8),
              ctx.config.username.getBytes(StandardCharsets.UTF_8)),
          salt);
      ctx.writeBytes(hash).writeByte((byte) 0).writeLengthIntEnd();
      ctx.buf.flip();
      return writeFrontendMessage().thenCompose(__ -> readAuthResponse());
    }

    protected CompletableFuture<QueryReadyConnection.AutoCommit> readPostAuthResponse() {
      return readNonGeneralBackendMessage().thenCompose(__ -> {
        char typ = (char) ctx.buf.get();
        switch (typ) {
          // BackendKeyData
          case 'K':
            ctx.buf.position(5);
            ctx.processId = ctx.buf.getInt();
            ctx.secretKey = ctx.buf.getInt();
            return readPostAuthResponse();
          // ReadyForQuery
          case 'Z':
            ctx.buf.position(5);
            updateReadyForQueryTransactionStatus();
            return CompletableFuture.completedFuture(new QueryReadyConnection.AutoCommit(ctx));
          // TODO: NegotiateProtocolVersion
          default: throw new IllegalArgumentException("Unrecognized post-auth response type: " + typ);
        }
      });
    }

    /**
     * Cancel another already-started connection using its {@link Started#getProcessId()} and
     * {@link Started#getSecretKey()}. This connection is closed after this call and can not tell the caller whether it
     * succeeded or failed.
     */
    public CompletableFuture<Void> cancelOther(int processId, int secretKey) {
      // Send the cancel request and just close the connection
      ctx.buf.clear();
      ctx.writeInt(16).writeInt(80877102).writeInt(processId).writeInt(secretKey);
      ctx.buf.flip();
      return writeFrontendMessage().whenComplete((__, ___) -> ctx.io.close());
    }
  }

  /** Base class for all authenticated connection states */
  public static abstract class Started extends Connection {
    protected @Nullable Started controlPassedTo;

    protected Started(Context ctx) { super(ctx); }

    /**
     * The process ID of this connection. Used in conjunction with {@link #getSecretKey()} for
     * {@link Startup#cancelOther(int, int)} for out-of-process cancellation.
     */
    public @Nullable Integer getProcessId() { return ctx.processId; }
    /**
     * The secret key of this connection. Used in conjunction with {@link #getProcessId()} for
     * {@link Startup#cancelOther(int, int)} for out-of-process cancellation.
     */
    public @Nullable Integer getSecretKey() { return ctx.secretKey; }

    /**
     * Basically just a check for general messages. This is useful for waiting for next subscribable item like
     * notifications. The resulting future will be errored if timeout is reached or if a non-general message is
     * received.
     */
    public CompletableFuture<Void> unsolicitedMessageTick(long timeout, TimeUnit timeoutUnit) {
      return readBackendMessage(timeout, timeoutUnit).thenCompose(__ -> {
        CompletableFuture<Void> generalHandled = handleGeneralResponse();
        if (generalHandled != null) return generalHandled;
        throw new DriverException.NonGeneralMessageOnTick((char) ctx.buf.get(0));
      });
    }

    protected void assertValid() {
      if (controlPassedTo != null) throw new IllegalStateException(
          "Attempting to reuse a connection in an invalid state. Did you forget a 'done' somewhere?");
    }

    protected <T extends Started> T passControlTo(T conn) {
      assertValid();
      this.controlPassedTo = conn;
      return conn;
    }

    protected void resumeControl() { controlPassedTo = null; }

    protected abstract CompletableFuture<QueryReadyConnection.AutoCommit> reset();

    /**
     * Completely reset this connection back to {@link QueryReadyConnection.AutoCommit} and ready to query regardless of
     * the state it is currently in. Among other uses, this is used by the connection pool to reset the connection.
     */
    public CompletableFuture<QueryReadyConnection.AutoCommit> fullReset() {
      ctx.ignoreErrors = true;
      CompletableFuture<QueryReadyConnection.AutoCommit> fut =
          controlPassedTo != null ? controlPassedTo.fullReset() : reset();
      return fut.whenComplete((__, ___) -> ctx.ignoreErrors = false);
    }
  }
}
