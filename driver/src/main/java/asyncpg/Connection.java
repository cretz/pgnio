package asyncpg;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public abstract class Connection implements AutoCloseable {
  protected final ConnectionContext ctx;

  protected Connection(ConnectionContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void close() {
    terminate();
  }

  public CompletableFuture<Void> terminate() {
    // Send terminate and close
    ctx.buf.clear();
    ctx.bufWriteByte((byte) 'X').bufLengthIntBegin().bufLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage().thenRun(ctx.io::close);
  }

  public Subscribable<Notice> notices() { return ctx.noticeSubscribable; }

  protected CompletableFuture<Void> readBackendMessage() {
    return readBackendMessage(ctx.config.defaultTimeout, ctx.config.defaultTimeoutUnit);
  }

  // The resulting buffer is only valid until next read/write
  protected CompletableFuture<Void> readBackendMessage(long timeout, TimeUnit timeoutUnit) {
    ctx.buf.clear();
    // We need 5 bytes to get the type and size
    ctx.bufEnsureCapacity(5).limit(5);
    return ctx.io.readFull(ctx.buf, timeout, timeoutUnit).thenCompose(__ -> {
      // Now that we have the size, make sure we have enough capacity to fulfill it and reset the limit
      int messageSize = ctx.buf.getInt(1);
      ctx.bufEnsureCapacity(messageSize).limit(1 + messageSize);
      // Fill it with the rest of the message and flip it for use
      return ctx.io.readFull(ctx.buf, timeout, timeoutUnit).thenRun(() -> ctx.buf.flip());
    });
  }

  protected CompletableFuture<Void> writeFrontendMessage() {
    return writeFrontendMessage(ctx.config.defaultTimeout, ctx.config.defaultTimeoutUnit);
  }

  protected CompletableFuture<Void> writeFrontendMessage(long timeout, TimeUnit timeoutUnit) {
    return ctx.io.writeFull(ctx.buf, timeout, timeoutUnit).thenRun(() -> ctx.buf.clear());
  }

  protected void assertNotErrorResponse() {
    if (ctx.buf.get(0) == 'E') {
      // Jump 5 then throw err
      ctx.buf.position(5);
      throw ServerException.fromContext(ctx);
    }
  }
}
