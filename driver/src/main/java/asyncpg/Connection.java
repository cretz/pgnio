package asyncpg;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class Connection implements AutoCloseable {
  public static final Logger log = Logger.getLogger(Connection.class.getName());

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
    return writeFrontendMessage().whenComplete((__, ___) -> ctx.io.close());
  }

  public Subscribable<Notice> notices() { return ctx.noticeSubscribable; }

  public Map<String, String> getRuntimeParameters() { return ctx.runtimeParameters; }

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
      if (log.isLoggable(Level.FINEST))
        log.log(Level.FINEST, "{0} Read message header of type {1} with size {2}",
            new Object[] { ctx, (char) ctx.buf.get(0), messageSize });
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

  // Returns null if not handled here
  protected CompletableFuture<Void> handleGeneralResponse() {
    char typ = (char) ctx.buf.get(0);
    switch (typ) {
      // Throw on error
      case 'E':
      case 'N':
        // Throw error or handle notice after skipping the length
        ctx.buf.position(5);
        Map<Byte, String> fields = new HashMap<>();
        while (true) {
          byte b = ctx.buf.get();
          if (b == 0) break;
          fields.put(b, ctx.bufReadString());
        }
        Notice notice = new Notice(fields);
        if (typ == 'E') throw new ServerException(notice);
        return notices().publish(notice);
      case 'S':
        // Handle status after skipping length
        ctx.buf.position(5);
        ctx.runtimeParameters.put(ctx.bufReadString(), ctx.bufReadString());
        return CompletableFuture.completedFuture(null);
      // TODO: Notification
      default: return null;
    }
  }

  protected CompletableFuture<Void> readNonGeneralBackendMessage() {
    return readBackendMessage().thenCompose(__ -> {
      CompletableFuture<Void> generalHandled = handleGeneralResponse();
      if (generalHandled == null) return CompletableFuture.completedFuture(null);
      return generalHandled.thenCompose(___ -> readNonGeneralBackendMessage());
    });
  }

  // Assumes buffer is at the status position
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
}
