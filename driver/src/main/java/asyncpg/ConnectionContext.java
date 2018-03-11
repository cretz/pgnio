package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ConnectionContext implements BufWriter {
  public final Config config;
  public final ConnectionIo io;
  protected ByteBuffer buf;
  protected int bufLastLengthBegin = -1;
  protected final Subscribable<Notice> noticeSubscribable = new Subscribable<>();
  protected final Subscribable<Notification> notificationSubscribable = new Subscribable<>();
  protected @Nullable Integer processId;
  protected @Nullable Integer secretKey;
  protected final Map<String, String> runtimeParameters = new HashMap<>();
  protected QueryReadyConnection.@Nullable TransactionStatus lastTransactionStatus;

  @SuppressWarnings("initialization")
  public ConnectionContext(Config config, ConnectionIo io) {
    this.config = config;
    this.io = io;
    buf = config.directBuffer ? ByteBuffer.allocateDirect(config.bufferStep) : ByteBuffer.allocate(config.bufferStep);
    // Add the notice log if we are logging em
    if (config.logNotices) noticeSubscribable.subscribe(n -> {
      n.log(Connection.log, this);
      return CompletableFuture.completedFuture(null);
    });
  }

  public ByteBuffer writeEnsureCapacity(int needed) {
    if (buf.capacity() - buf.position() < needed) {
      // Round up to the next step and then add it
      int newAmount = (((buf.capacity() + needed) / config.bufferStep) + 1) * config.bufferStep;
      buf = (config.directBuffer ? ByteBuffer.allocateDirect(newAmount) : ByteBuffer.allocate(newAmount)).put(buf);
    }
    return buf;
  }

  @Override
  public ConnectionContext writeLengthIntBegin() {
    if (bufLastLengthBegin != -1) throw new IllegalStateException("Length already started");
    bufLastLengthBegin = buf.position();
    return writeInt(0);
  }

  @Override
  public ConnectionContext writeLengthIntEnd() {
    if (bufLastLengthBegin == -1) throw new IllegalStateException("Length not started");
    buf.putInt(bufLastLengthBegin, buf.position() - bufLastLengthBegin);
    bufLastLengthBegin = -1;
    return this;
  }

  @Override
  public ConnectionContext writeByte(byte b) {
    writeEnsureCapacity(1).put(b);
    return this;
  }

  @Override
  public ConnectionContext writeBytes(byte[] b) {
    writeEnsureCapacity(b.length).put(b);
    return this;
  }

  @Override
  public ConnectionContext writeShort(short s) {
    writeEnsureCapacity(2).putShort(s);
    return this;
  }

  @Override
  public ConnectionContext writeInt(int i) {
    writeEnsureCapacity(4).putInt(i);
    return this;
  }

  @Override
  public ConnectionContext writeString(String str) {
    ByteBuffer strBuf;
    try {
      strBuf = Util.threadLocalStringEncoder.get().encode(CharBuffer.wrap(str));
    } catch (CharacterCodingException e) { throw new IllegalArgumentException(e); }
    writeEnsureCapacity(strBuf.limit() + 1).put(strBuf).put((byte) 0);
    return this;
  }

  public String bufReadString() {
    int indexOfZero = buf.position();
    while (buf.get(indexOfZero) != 0) indexOfZero++;
    // Temporarily put the limit for decoding
    int prevLimit = buf.limit();
    buf.limit(indexOfZero);
    String ret;
    try {
      ret = Util.threadLocalStringDecoder.get().decode(buf).toString();
    } catch (CharacterCodingException e) { throw new IllegalStateException(e); }
    buf.limit(prevLimit);
    // Read the zero
    buf.position(buf.position() + 1);
    return ret;
  }

  // Mostly just for the benefit of loggers
  @Override
  public String toString() {
    return "[" + config.username + "@" + config.hostname + ":" + config.port + "->" +
        io.getLocalPort() + "/" + config.database + "]";
  }
}
