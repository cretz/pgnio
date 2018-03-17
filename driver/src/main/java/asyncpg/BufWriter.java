package asyncpg;

import java.nio.ByteBuffer;

public interface BufWriter {
  BufWriter writeLengthIntBegin();
  BufWriter writeLengthIntEnd();
  BufWriter writeByte(byte b);
  BufWriter writeBytes(byte[] b);
  BufWriter writeShort(short s);
  BufWriter writeInt(int i);
  BufWriter writeStringEscapeSingleQuoteBegin();
  BufWriter writeStringEscapeSingleQuoteEnd();
  BufWriter writeStringEscapeDoubleQuoteBegin();
  BufWriter writeStringEscapeDoubleQuoteEnd();
  BufWriter writeString(String str);
  BufWriter writeCString(String str);

  @SuppressWarnings("unchecked")
  class Simple<SELF extends Simple<SELF>> implements BufWriter {
    public final boolean directBuffer;
    public final int bufferStep;
    public ByteBuffer buf;
    protected int bufLastLengthBegin = -1;
    protected boolean escapeSingleQuote;
    protected int escapeDoubleQuoteDepth;

    public Simple(boolean directBuffer, int bufferStep) {
      this.directBuffer = directBuffer;
      this.bufferStep = bufferStep;
      buf = directBuffer ? ByteBuffer.allocateDirect(bufferStep) : ByteBuffer.allocate(bufferStep);
    }

    public ByteBuffer writeEnsureCapacity(int needed) {
      if (buf.capacity() - buf.position() < needed) {
        // Round up to the next step and then add it
        int newAmount = (((buf.capacity() + needed) / bufferStep) + 1) * bufferStep;
        buf = (directBuffer ? ByteBuffer.allocateDirect(newAmount) : ByteBuffer.allocate(newAmount)).put(buf);
      }
      return buf;
    }

    @Override
    public SELF writeLengthIntBegin() {
      if (bufLastLengthBegin != -1) throw new IllegalStateException("Length already started");
      bufLastLengthBegin = buf.position();
      return writeInt(0);
    }

    @Override
    public SELF writeLengthIntEnd() {
      if (bufLastLengthBegin == -1) throw new IllegalStateException("Length not started");
      buf.putInt(bufLastLengthBegin, buf.position() - bufLastLengthBegin);
      bufLastLengthBegin = -1;
      return (SELF) this;
    }

    @Override
    public SELF writeByte(byte b) {
      writeEnsureCapacity(1).put(b);
      return (SELF) this;
    }

    @Override
    public SELF writeBytes(byte[] b) {
      writeEnsureCapacity(b.length).put(b);
      return (SELF) this;
    }

    @Override
    public SELF writeShort(short s) {
      writeEnsureCapacity(2).putShort(s);
      return (SELF) this;
    }

    @Override
    public SELF writeInt(int i) {
      writeEnsureCapacity(4).putInt(i);
      return (SELF) this;
    }

    @Override
    public SELF writeStringEscapeSingleQuoteBegin() {
      if (escapeSingleQuote) throw new IllegalStateException("Already escaping single quote");
      escapeSingleQuote = true;
      return (SELF) this;
    }

    @Override
    public SELF writeStringEscapeSingleQuoteEnd() {
      if (!escapeSingleQuote) throw new IllegalStateException("Not escaping single quote");
      escapeSingleQuote = false;
      return (SELF) this;
    }

    @Override
    public SELF writeStringEscapeDoubleQuoteBegin() {
      escapeDoubleQuoteDepth++;
      return (SELF) this;
    }

    @Override
    public SELF writeStringEscapeDoubleQuoteEnd() {
      if (escapeDoubleQuoteDepth == 0) throw new IllegalStateException("Not escaping double quote");
      escapeDoubleQuoteDepth--;
      return (SELF) this;
    }

    @Override
    public SELF writeString(String str) {
      if (escapeSingleQuote) str = str.replace("'", "''");
      if (escapeDoubleQuoteDepth > 0) str = str.replace("\\", "\\\\").replace("\"", "\\\"");
      ByteBuffer strBuf = Util.byteBufferFromString(str);
      writeEnsureCapacity(strBuf.limit()).put(strBuf);
      return (SELF) this;
    }

    @Override
    public SELF writeCString(String str) {
      return writeString(str).writeByte((byte) 0);
    }
  }
}
