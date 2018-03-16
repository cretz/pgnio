package asyncpg;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public interface ConnectionIo {
  CompletableFuture<Void> close();

  // -1 if not connected
  int getLocalPort();

  default CompletableFuture<Void> readFull(ByteBuffer buf, long timeout, TimeUnit timeoutUnit) {
    return readSome(buf, timeout, timeoutUnit).thenCompose(__ -> {
      if (buf.hasRemaining()) return readFull(buf, timeout, timeoutUnit);
      return CompletableFuture.completedFuture(null);
    });
  }

  CompletableFuture<Void> readSome(ByteBuffer buf, long timeout, TimeUnit timeoutUnit);

  CompletableFuture<Void> writeFull(ByteBuffer buf, long timeout, TimeUnit timeoutUnit);

  class AsyncSocketChannel implements ConnectionIo {
    private static final Logger log = Logger.getLogger(AsyncSocketChannel.class.getName());

    public static CompletableFuture<AsyncSocketChannel> connect(Config config) {
      try {
        CompletableFuture<Void> ret = new CompletableFuture<>();
        AsynchronousSocketChannel ch = AsynchronousSocketChannel.open();
        ch.connect(new InetSocketAddress(config.hostname, config.port), null, Util.handlerFromFuture(ret));
        return ret.thenApply(__ -> new AsyncSocketChannel(ch));
      } catch (IOException e) { throw new RuntimeException(e); }
    }

    protected final AsynchronousSocketChannel ch;

    protected AsyncSocketChannel(AsynchronousSocketChannel ch) {
      this.ch = ch;
    }

    @Override
    public CompletableFuture<Void> close() {
      try {
        ch.close();
        return CompletableFuture.completedFuture(null);
      } catch (IOException e) {
        CompletableFuture<Void> ret = new CompletableFuture<>();
        ret.completeExceptionally(e);
        return ret;
      }
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
      return readSome(buf, timeout, timeoutUnit).thenCompose(__ -> {
        if (buf.hasRemaining()) return readFull(buf, timeout, timeoutUnit);
        return CompletableFuture.completedFuture(null);
      });
    }

    @Override
    public CompletableFuture<Void> readSome(ByteBuffer buf, long timeout, TimeUnit timeoutUnit) {
      CompletableFuture<Void> ret = new CompletableFuture<>();
      if (log.isLoggable(Level.FINEST)) log.log(Level.FINEST, "Reading bytes {0}", buf);
      ch.read(buf, timeout, timeoutUnit, buf, new CompletionHandler<Integer, ByteBuffer>() {
        @Override
        public void completed(Integer result, ByteBuffer buf) {
          if (result == -1) ret.completeExceptionally(new IllegalStateException("Channel closed"));
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

  class Ssl implements ConnectionIo {
    // Lots of help from, among others, https://github.com/jesperdj/sslclient
    protected static final Logger log = Logger.getLogger(Ssl.class.getName());

    protected static ByteBuffer allocBuf(boolean directBuffer, int amount) {
      if (directBuffer) return ByteBuffer.allocateDirect(amount);
      return ByteBuffer.allocate(amount);
    }

    protected final ConnectionIo underlying;
    protected final SSLEngine sslEngine;
    protected final boolean directBuffer;
    protected final long defaultTimeout;
    protected final TimeUnit defaultTimeoutUnit;
    protected ByteBuffer appReadBuf;
    protected ByteBuffer appWriteBuf;
    protected ByteBuffer netReadBuf;
    protected ByteBuffer netWriteBuf;

    public Ssl(ConnectionIo underlying, SSLEngine sslEngine, boolean directBuffer,
        long defaultTimeout, TimeUnit defaultTimeoutUnit) {
      this.underlying = underlying;
      sslEngine.setUseClientMode(true);
      this.sslEngine = sslEngine;
      this.directBuffer = directBuffer;
      this.defaultTimeout = defaultTimeout;
      this.defaultTimeoutUnit = defaultTimeoutUnit;

      appReadBuf = allocBuf(directBuffer, sslEngine.getSession().getApplicationBufferSize());
      appWriteBuf = allocBuf(directBuffer, sslEngine.getSession().getApplicationBufferSize());
      netReadBuf = allocBuf(directBuffer, sslEngine.getSession().getPacketBufferSize());
      netWriteBuf = allocBuf(directBuffer, sslEngine.getSession().getPacketBufferSize());
    }

    @Override
    public CompletableFuture<Void> close() {
      sslEngine.closeOutbound();
      return handshakeUpdate(defaultTimeout, defaultTimeoutUnit).thenCompose(__ -> underlying.close());
    }

    @Override
    public int getLocalPort() { return underlying.getLocalPort(); }

    public CompletableFuture<Void> start() {
      log.log(Level.FINER, "Starting SSL handshake");
      try {
        sslEngine.beginHandshake();
      } catch (SSLException e) { throw new RuntimeException(e); }
      return handshakeUpdate(defaultTimeout, defaultTimeoutUnit);
    }

    @Override
    public CompletableFuture<Void> readSome(ByteBuffer buf, long timeout, TimeUnit timeoutUnit) {
      appReadBuf.flip();
      if (log.isLoggable(Level.FINEST))
        log.log(Level.FINEST, "Reading SSL into {0} from app buf {1}", new Object[] { buf, appReadBuf });
      // If there is anything, we use it
      if (appReadBuf.hasRemaining()) {
        Integer prevLimit = null;
        // If we can full up the buf, even better
        if (appReadBuf.remaining() >= buf.remaining()) {
          prevLimit = appReadBuf.limit();
          appReadBuf.limit(appReadBuf.position() + buf.remaining());
        }
        buf.put(appReadBuf);
        if (prevLimit != null) appReadBuf.limit(prevLimit);
        appReadBuf.compact();
        return CompletableFuture.completedFuture(null);
      }
      // Otherwise, unwrap and try again
      return unwrap(timeout, timeoutUnit).thenCompose(__ -> readSome(buf, timeout, timeoutUnit));
    }

    @Override
    public CompletableFuture<Void> writeFull(ByteBuffer buf, long timeout, TimeUnit timeoutUnit) {
      ensureRemaining(appWriteBuf, buf.remaining());
      appWriteBuf.put(buf);
      return wrap(timeout, timeoutUnit);
    }

    protected CompletableFuture<Void> handshakeUpdate(long timeout, TimeUnit timeoutUnit) {
      return handshakeUpdate(timeout, timeoutUnit, sslEngine.getHandshakeStatus());
    }

    protected CompletableFuture<Void> handshakeUpdate(long timeout, TimeUnit timeoutUnit,
        SSLEngineResult.HandshakeStatus handshakeStatus) {
      log.log(Level.FINEST, "Handshake update for {0}", handshakeStatus);
      switch (handshakeStatus) {
        case NOT_HANDSHAKING:
          return CompletableFuture.completedFuture(null);
        case FINISHED:
          if (log.isLoggable(Level.FINE))
            log.log(Level.FINE, "Handshake finished, protocol: {0}", sslEngine.getSession().getProtocol());
          return CompletableFuture.completedFuture(null);
        case NEED_WRAP:
          return wrap(timeout, timeoutUnit);
        case NEED_UNWRAP:
          return unwrap(timeout, timeoutUnit);
        case NEED_TASK:
          while (true) {
            Runnable task = sslEngine.getDelegatedTask();
            if (task == null) break;
            log.log(Level.FINEST, "Running task: {0}", task);
            task.run();
          }
          return handshakeUpdate(timeout, timeoutUnit);
        default:
          throw new IllegalStateException("Unknown status: " + handshakeStatus);
      }
    }

    protected ByteBuffer ensureRemaining(ByteBuffer buf, int needed) {
      if (buf.remaining() >= needed) return buf;
      buf.flip();
      return allocBuf(directBuffer, buf.remaining() + needed).put(buf);
    }

    protected CompletableFuture<Void> wrap(long timeout, TimeUnit timeoutUnit) {
      appWriteBuf.flip();
      if (log.isLoggable(Level.FINEST))
        log.log(Level.FINEST, "Wrap from {0} into {1}", new Object[] { appWriteBuf, netWriteBuf });
      SSLEngineResult result;
      try {
        result = sslEngine.wrap(appWriteBuf, netWriteBuf);
      } catch (SSLException e) { throw new RuntimeException(e); }
      appWriteBuf.compact();
      if (log.isLoggable(Level.FINEST))
        log.log(Level.FINEST, "Wrap result {0} after {1} into {2}", new Object[] { result, appWriteBuf, netWriteBuf });
      switch (result.getStatus()) {
        case OK:
          return flushNetWriteBuf(timeout, timeoutUnit).
              thenCompose(__ -> handshakeUpdate(timeout, timeoutUnit, result.getHandshakeStatus())).
              thenCompose(__ -> {
                // Wrap again if there's more
                if (appWriteBuf.position() > 0) return wrap(timeout, timeoutUnit);
                return CompletableFuture.completedFuture(null);
              });
        case CLOSED:
          return flushNetWriteBuf(timeout, timeoutUnit).
              thenCompose(__ -> handshakeUpdate(timeout, timeoutUnit, result.getHandshakeStatus())).
              thenCompose(__ -> close());
        case BUFFER_OVERFLOW:
          netWriteBuf = ensureRemaining(netWriteBuf, sslEngine.getSession().getPacketBufferSize());
          return wrap(timeout, timeoutUnit);
        default:
          throw new IllegalStateException("Unknown status: " + result.getStatus());
      }
    }

    protected CompletableFuture<Void> flushNetWriteBuf(long timeout, TimeUnit timeoutUnit) {
      netWriteBuf.flip();
      log.log(Level.FINEST, "Flushing {0}", netWriteBuf);
      return underlying.writeFull(netWriteBuf, timeout, timeoutUnit).thenRun(netWriteBuf::clear);
    }

    protected CompletableFuture<Void> unwrap(long timeout, TimeUnit timeoutUnit) {
      log.log(Level.FINEST, "Unwrap begin on {0}", netReadBuf);
      // If net buf is empty, do read first
      CompletableFuture<Void> readComplete;
      if (netReadBuf.position() == 0) readComplete = underlying.readSome(netReadBuf, timeout, timeoutUnit);
      else readComplete = CompletableFuture.completedFuture(null);
      return readComplete.thenCompose(__ -> {
        netReadBuf.flip();
        if (log.isLoggable(Level.FINEST))
          log.log(Level.FINEST, "Unwrap from {0} into {1}", new Object[] { netReadBuf, appReadBuf });
        SSLEngineResult result;
        try {
          result = sslEngine.unwrap(netReadBuf, appReadBuf);
        } catch (SSLException e) { throw new RuntimeException(e); }
        if (log.isLoggable(Level.FINEST))
          log.log(Level.FINEST, "Unwrap result {0} after {1} into {2}",
              new Object[] { result, netReadBuf, appReadBuf });
        netReadBuf.compact();
        switch (result.getStatus()) {
          case OK:
            return handshakeUpdate(timeout, timeoutUnit, result.getHandshakeStatus());
          case CLOSED:
            return handshakeUpdate(timeout, timeoutUnit, result.getHandshakeStatus()).thenCompose(___ -> close());
          case BUFFER_UNDERFLOW:
            // Increase net buf size and retry
            netReadBuf = ensureRemaining(netReadBuf, sslEngine.getSession().getPacketBufferSize());
            return underlying.readSome(netReadBuf, timeout, timeoutUnit).
                thenCompose(___ -> unwrap(timeout, timeoutUnit));
          case BUFFER_OVERFLOW:
            // Increase temp buf and try again
            appReadBuf = ensureRemaining(appReadBuf, sslEngine.getSession().getApplicationBufferSize());
            return unwrap(timeout, timeoutUnit);
          default:
            throw new IllegalStateException("Unknown status: " + result.getStatus());
        }
      });
    }
  }
}
