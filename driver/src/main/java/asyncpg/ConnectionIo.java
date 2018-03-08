package asyncpg;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface ConnectionIo extends AutoCloseable {
  // It's ok if this throws, just not a checked exception
  @Override
  void close() throws RuntimeException;

  // -1 if not connected
  int getLocalPort();

  CompletableFuture<Void> readFull(ByteBuffer buf, long timeout, TimeUnit timeoutUnit);
  CompletableFuture<Void> writeFull(ByteBuffer buf, long timeout, TimeUnit timeoutUnit);
}
