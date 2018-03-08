package asyncpg;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

public class StartupConnection extends Connection {
  public static CompletableFuture<StartupConnection> init(Config config) {
    log.log(Level.FINE, "Connecting to {0}", config.hostname);
    return config.connector.get().thenApply(io -> new StartupConnection(config, io));
  }

  protected StartupConnection(Config config, ConnectionIo io) {
    super(new ConnectionContext(config, io));
  }

  public CompletableFuture<QueryReadyConnection.AutoCommit> auth() {
    log.log(Level.FINE, "{0} Authenticating", ctx);
    // Send startup message
    ctx.buf.clear();
    ctx.bufLengthIntBegin().bufWriteInt(ctx.config.protocolVersion).bufWriteString("user").
        bufWriteString(ctx.config.username);
    if (ctx.config.database != null) ctx.bufWriteString("database").bufWriteString(ctx.config.database);
    if (ctx.config.additionalStartupParams != null)
      ctx.config.additionalStartupParams.forEach((k, v) -> ctx.bufWriteString(k).bufWriteString(v));
    ctx.bufWriteByte((byte) 0).bufLengthIntEnd();
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
    ctx.bufWriteByte((byte) 'p').bufLengthIntBegin().bufWriteString(ctx.config.password).bufLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage().thenCompose(__ -> readAuthResponse());
  }

  protected CompletableFuture<QueryReadyConnection.AutoCommit> sendMd5Password(byte... salt) {
    // "md5" + md5(md5(password + username) + random-salt))
    ctx.buf.clear();
    ctx.bufWriteByte((byte) 'p').bufLengthIntBegin().
        bufWriteByte((byte) 'm').bufWriteByte((byte) 'd').bufWriteByte((byte) '5');
    MessageDigest md5;
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) { throw new RuntimeException(e); }
    byte[] hash = Util.md5Hex(md5,
        Util.md5Hex(md5, ctx.config.password.getBytes(StandardCharsets.UTF_8),
            ctx.config.username.getBytes(StandardCharsets.UTF_8)),
        salt);
    ctx.bufWriteBytes(hash).bufWriteByte((byte) 0).bufLengthIntEnd();
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

  protected CompletableFuture<Void> cancelOther(int processId, int secretKey) {
    throw new UnsupportedOperationException();
  }
}
