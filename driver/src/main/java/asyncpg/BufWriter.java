package asyncpg;

import java.nio.ByteBuffer;

/** Wrapper for a growable output buffer */
public interface BufWriter {
  /**
   * Mark the current 4 bytes and move past them. When {@link #writeLengthIntEnd()} is invoked, the amount of bytes
   * written between this call and that call will be serialized at the marked point. It is an error to call this a
   * second time before {@link #writeLengthIntEnd()} is called.
   */
  BufWriter writeLengthIntBegin();

  /**
   * End a {@link #writeLengthIntBegin()}. It is an error to call this if {@link #writeLengthIntBegin()} wasn't called.
   */
  BufWriter writeLengthIntEnd();

  /** Write a single byte */
  BufWriter writeByte(byte b);

  /** Write the entire byte array */
  BufWriter writeBytes(byte[] b);

  /** Write the short value as two bytes */
  BufWriter writeShort(short s);

  /** Write the int value as four bytes */
  BufWriter writeInt(int i);

  /**
   * Make all future {@link #writeString(String)} and {@link #writeCString(String)} calls escape single quotes within
   * them until {@link #writeStringEscapeSingleQuoteEnd()} is called. It is an error to call this a second time before
   * {@link #writeStringEscapeSingleQuoteEnd()} is called.
   */
  BufWriter writeStringEscapeSingleQuoteBegin();

  /** Stop escaping single quotes as was started in {@link #writeStringEscapeSingleQuoteBegin()} */
  BufWriter writeStringEscapeSingleQuoteEnd();

  /**
   * Make all future {@link #writeString(String)} and {@link #writeCString(String)} calls escape double quotes and
   * backslashes within them until {@link #writeStringEscapeDoubleQuoteEnd()} is called. It is not an error to call this
   * multiple times, but {@link #writeStringEscapeDoubleQuoteEnd()} must be called the same amount of times afterwards.
   * The escaping continues until the last {@link #writeStringEscapeDoubleQuoteEnd()} is called.
   */
  BufWriter writeStringEscapeDoubleQuoteBegin();

  /**
   * Stop escaping double quotes as was started in {@link #writeStringEscapeDoubleQuoteBegin()}. If
   * {@link #writeStringEscapeDoubleQuoteBegin()} was called multiple times, only the last matching call here will stop
   * escaping. It is an error to call this more times than {@link #writeStringEscapeDoubleQuoteBegin()} was called.
   */
  BufWriter writeStringEscapeDoubleQuoteEnd();

  /** Write a string of bytes, not terminating with a null char */
  BufWriter writeString(String str);

  /** Same as {@link #writeString(String)} but adds a null char at the end */
  BufWriter writeCString(String str);

  /** Implementation of {@link BufWriter} for a {@link ByteBuffer} */
  @SuppressWarnings("unchecked")
  class Simple<SELF extends Simple<SELF>> implements BufWriter {
    /** Whether or not the created byte buffers are "direct" */
    public final boolean directBuffer;
    /** The amount of bytes to increase the size by on buffer growth */
    public final int bufferStep;
    /** The underlying buffer. Do not use this directly. */
    public ByteBuffer buf;
    /** Position when {@link #writeLengthIntBegin()} was called or -1 otherwise */
    protected int bufLastLengthBegin = -1;
    /** Whether or not single quotes are being escaped via {@link #writeStringEscapeSingleQuoteBegin()} */
    protected boolean escapeSingleQuote;
    /** The number of times {@link #writeStringEscapeDoubleQuoteBegin()} is called without end */
    protected int escapeDoubleQuoteDepth;

    /** Create writer. See {@link #directBuffer} and {@link #bufferStep} */
    public Simple(boolean directBuffer, int bufferStep) {
      this.directBuffer = directBuffer;
      this.bufferStep = bufferStep;
      buf = directBuffer ? ByteBuffer.allocateDirect(bufferStep) : ByteBuffer.allocate(bufferStep);
    }

    /**
     * Increase buffer size to have the needed amount available. This is done via new buffer creation, so old contents
     * are copied and position is retained. Result is convenience for {@link #buf}.
     */
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
