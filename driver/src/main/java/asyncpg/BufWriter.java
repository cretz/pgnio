package asyncpg;

public interface BufWriter {
  BufWriter writeLengthIntBegin();
  BufWriter writeLengthIntEnd();
  BufWriter writeByte(byte b);
  BufWriter writeBytes(byte[] b);
  BufWriter writeShort(short s);
  BufWriter writeInt(int i);
  BufWriter writeString(String str);
}
