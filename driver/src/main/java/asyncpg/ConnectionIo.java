package asyncpg;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public interface ConnectionIo extends AutoCloseable {
  // It's ok if this throws, just not a checked exception
  @Override
  void close() throws RuntimeException;

  // -1 if not connected
  int getLocalPort();

  CompletableFuture<Void> readFull(ByteBuffer buf, long timeout, TimeUnit timeoutUnit);
  CompletableFuture<Void> writeFull(ByteBuffer buf, long timeout, TimeUnit timeoutUnit);

  class AsynchronousSocketChannel implements ConnectionIo {
    private static final Logger log = Logger.getLogger(AsynchronousSocketChannel.class.getName());

    public static CompletableFuture<AsynchronousSocketChannel> connect(Config config) {
      try {
        CompletableFuture<Void> ret = new CompletableFuture<>();
        java.nio.channels.AsynchronousSocketChannel ch = java.nio.channels.AsynchronousSocketChannel.open();
        ch.connect(new InetSocketAddress(config.hostname, config.port), null, Util.handlerFromFuture(ret));
        return ret.thenApply(__ -> new AsynchronousSocketChannel(ch));
      } catch (IOException e) { throw new RuntimeException(e); }
    }

    protected final java.nio.channels.AsynchronousSocketChannel ch;

    protected AsynchronousSocketChannel(java.nio.channels.AsynchronousSocketChannel ch) {
      this.ch = ch;
    }

    @Override
    public void close() {
      try {
        ch.close();
      } catch (IOException e) { throw new RuntimeException(e); }
    }

    @Override
    public int getLocalPort() {
      try {
        SocketAddress addr = ch.getLocalAddress();
        if (addr instanceof InetSocketAddress) return ((InetSocketAddress) addr).getPort();
      } catch (Exception e) {
        // Ignore errors
      }
      return -1;
    }

    @Override
    public CompletableFuture<Void> readFull(ByteBuffer buf, long timeout, TimeUnit timeoutUnit) {
      CompletableFuture<Void> ret = new CompletableFuture<>();
      if (log.isLoggable(Level.FINEST)) log.log(Level.FINEST, "Reading bytes {0}", buf);
      ch.read(buf, timeout, timeoutUnit, buf, new CompletionHandler<Integer, ByteBuffer>() {
        @Override
        public void completed(Integer result, ByteBuffer buf) {
          if (result == -1) ret.completeExceptionally(new IllegalStateException("Channel closed"));
          else if (buf.hasRemaining()) ch.read(buf, timeout, timeoutUnit, buf, this);
          else ret.complete(null);
        }

        @Override
        public void failed(Throwable exc, ByteBuffer buf) { ret.completeExceptionally(exc); }
      });
      return ret;
    }

    @Override
    public CompletableFuture<Void> writeFull(ByteBuffer buf, long timeout, TimeUnit timeoutUnit) {
      CompletableFuture<Void> ret = new CompletableFuture<>();
      if (log.isLoggable(Level.FINEST)) log.log(Level.FINEST, "Writing bytes {0}", buf);
      ch.write(buf, timeout, timeoutUnit, buf, new CompletionHandler<Integer, ByteBuffer>() {
        @Override
        public void completed(Integer result, ByteBuffer buf) {
          if (buf.hasRemaining()) ch.write(buf, timeout, timeoutUnit, buf, this);
          else ret.complete(null);
        }

        @Override
        public void failed(Throwable exc, ByteBuffer buf) { ret.completeExceptionally(exc); }
      });
      return ret;
    }
  }
}
