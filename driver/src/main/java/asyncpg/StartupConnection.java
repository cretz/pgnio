package asyncpg;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;

public class StartupConnection extends Connection {
  public static CompletableFuture<StartupConnection> init(Config config) {
    return config.connector.get().thenApply(io -> new StartupConnection(config, io));
  }

  protected StartupConnection(Config config, ConnectionIo io) {
    super(new ConnectionContext(config, io));
  }

  public CompletableFuture<QueryReadyConnection.AutoCommit> auth() {
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
    return readBackendMessage().thenCompose(__ -> {
      assertNotErrorResponse();
      char typ = (char) ctx.buf.get();
      if (typ != 'R') throw new IllegalArgumentException("Unrecognized auth response message: " + typ);
      // Skip the length, grab the auth type
      ctx.buf.position(5);
      int authType = ctx.buf.getInt();
      switch (authType) {
        // AuthenticationOk
        case 0: return CompletableFuture.completedFuture(new QueryReadyConnection.AutoCommit(ctx));
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

  protected CompletableFuture<Void> cancelOther(int processId, int secretKey) {
    throw new UnsupportedOperationException();
  }
}
