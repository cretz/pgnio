package asyncpg;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;

public class ConnectionContext {
  public final Config config;
  public final ConnectionIo io;
  protected ByteBuffer buf;
  protected int bufLastLengthBegin = -1;
  public final Subscribable<Notice> noticeSubscribable = new Subscribable<>();
  public final Subscribable<Notification> notificationSubscribable = new Subscribable<>();

  public ConnectionContext(Config config, ConnectionIo io) {
    this.config = config;
    this.io = io;
    buf = config.directBuffer ? ByteBuffer.allocateDirect(config.bufferStep) : ByteBuffer.allocate(config.bufferStep);
  }

  public ByteBuffer bufEnsureCapacity(int needed) {
    if (buf.remaining() < needed) {
      // Round up to the next step and then add it
      int newAmount = (((buf.capacity() + needed) / config.bufferStep) + 1) * config.bufferStep;
      buf = (config.directBuffer ? ByteBuffer.allocateDirect(newAmount) : ByteBuffer.allocate(newAmount)).put(buf);
    }
    return buf;
  }

  public ConnectionContext bufLengthIntBegin() {
    if (bufLastLengthBegin != -1) throw new IllegalStateException("Length already started");
    bufLastLengthBegin = buf.position() + 4;
    return bufWriteInt(0);
  }

  public ConnectionContext bufLengthIntEnd() {
    if (bufLastLengthBegin == -1) throw new IllegalStateException("Length not started");
    buf.putInt(bufLastLengthBegin, buf.position() - bufLastLengthBegin);
    bufLastLengthBegin = -1;
    return this;
  }

  public ConnectionContext bufWriteByte(byte b) {
    bufEnsureCapacity(1).put(b);
    return this;
  }

  public ConnectionContext bufWriteBytes(byte[] b) {
    bufEnsureCapacity(b.length).put(b);
    return this;
  }

  public ConnectionContext bufWriteShort(short s) {
    bufEnsureCapacity(2).putShort(s);
    return this;
  }

  public ConnectionContext bufWriteInt(int i) {
    bufEnsureCapacity(4).putInt(i);
    return this;
  }

  public ConnectionContext bufWriteString(String str) {
    ByteBuffer strBuf;
    try {
      strBuf = Util.threadLocalStringEncoder.get().encode(CharBuffer.wrap(str));
    } catch (CharacterCodingException e) { throw new IllegalArgumentException(e); }
    bufEnsureCapacity(strBuf.limit() + 1).put(strBuf).put((byte) 0);
    return this;
  }

  public String bufReadString() {
    int indexOfZero = buf.position();
    while (buf.get(indexOfZero) != 0) indexOfZero++;
    ByteBuffer strBuf = buf.slice();
    buf.limit(indexOfZero - buf.position());
    buf.position(indexOfZero + 1);
    try {
      return Util.threadLocalStringDecoder.get().decode(strBuf).toString();
    } catch (CharacterCodingException e) { throw new IllegalStateException(e); }
  }
}
