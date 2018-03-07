package asyncpg.driver;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

interface FrontendMessage {
  void marshal(MarshalContext ctx);

  class Bind implements FrontendMessage {
    final String portal;
    final String statementName;
    final boolean[] parametersFormatText;
    final byte[][] parameters;
    final boolean[] resultsFormatText;

    Bind(String portal, String statementName, boolean[] parametersFormatText,
        byte[][] parameters, boolean[] resultsFormatText) {
      this.portal = portal;
      this.statementName = statementName;
      this.parametersFormatText = parametersFormatText;
      this.parameters = parameters;
      this.resultsFormatText = resultsFormatText;
    }

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeByte((byte) 'B').lengthIntBegin().writeString(portal).writeString(statementName).
          writeShort((short) parametersFormatText.length);
      for (boolean parameterFormatText : parametersFormatText) ctx.writeShort((short) (parameterFormatText ? 0 : 1));
      ctx.writeShort((short) parameters.length);
      for (byte[] parameter : parameters) {
        if (parameter == null) ctx.writeInt(-1);
        else ctx.writeInt(parameter.length).writeBytes(parameter);
      }
      for (boolean resultFormatText : resultsFormatText) ctx.writeShort((short) (resultFormatText ? 0 : 1));
    }
  }

  class CancelRequest implements FrontendMessage {
    final int processId;
    final int secretKey;

    CancelRequest(int processId, int secretKey) {
      this.processId = processId;
      this.secretKey = secretKey;
    }

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeInt(16).writeInt(80877102).writeInt(processId).writeInt(secretKey);
    }
  }

  class Close implements FrontendMessage {
    final boolean portal;
    final String name;

    Close(boolean portal, String name) {
      this.portal = portal;
      this.name = name;
    }

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeByte((byte) 'C').lengthIntBegin().writeByte((byte) (portal ? 'P' : 'S')).
          writeString(name).lengthIntEnd();
    }
  }

  class CopyData implements FrontendMessage {
    final byte[] bytes;

    CopyData(byte[] bytes) { this.bytes = bytes; }

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeByte((byte) 'd').lengthIntBegin().writeBytes(bytes).lengthIntEnd();
    }
  }

  class CopyDone implements FrontendMessage {
    static final CopyDone INSTANCE = new CopyDone();

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeByte((byte) 'c').lengthIntBegin().lengthIntEnd();
    }
  }

  class CopyFail implements FrontendMessage {
    final String message;

    CopyFail(String message) { this.message = message; }

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeByte((byte) 'f').lengthIntBegin().writeString(message).lengthIntEnd();
    }
  }

  class Describe implements FrontendMessage {
    final boolean portal;
    final String name;

    Describe(boolean portal, String name) {
      this.portal = portal;
      this.name = name;
    }

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeByte((byte) 'D').lengthIntBegin().writeByte((byte) (portal ? 'P' : 'S')).
          writeString(name).lengthIntEnd();
    }
  }

  class Execute implements FrontendMessage {
    final String portal;
    final int maxRows;

    Execute(String portal, int maxRows) {
      this.portal = portal;
      this.maxRows = maxRows;
    }

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeByte((byte) 'E').lengthIntBegin().writeString(portal).writeInt(maxRows).lengthIntEnd();
    }
  }

  class Flush implements FrontendMessage {
    static final Flush INSTANCE = new Flush();

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeByte((byte) 'H').lengthIntBegin().lengthIntEnd();
    }
  }

  class Parse implements FrontendMessage {
    final String name;
    final String query;
    final int[] parameterDataTypeOids;

    Parse(String query) {
      this(query, new int[0]);
    }

    Parse(String query, int[] parameterDataTypeOids) {
      this("", query, parameterDataTypeOids);
    }

    Parse(String name, String query, int[] parameterDataTypeOids) {
      this.name = name;
      this.query = query;
      this.parameterDataTypeOids = parameterDataTypeOids;
    }

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeByte((byte) 'P').lengthIntBegin().writeString(name).writeString(query).
          writeShort((short) parameterDataTypeOids.length);
      for (int parameterDataTypeOid : parameterDataTypeOids) ctx.writeInt(parameterDataTypeOid);
      ctx.lengthIntEnd();
    }
  }

  class PasswordMessage implements FrontendMessage {
    static PasswordMessage md5Hashed(String username, String password, byte[] salt) {
      MessageDigest md5;
      try {
        md5 = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) { throw new RuntimeException(e); }
      byte[] hashSuffix = Util.md5Hex(md5,
          Util.md5Hex(md5, password.getBytes(StandardCharsets.UTF_8), username.getBytes(StandardCharsets.UTF_8)),
          salt);
      byte[] hash = new byte[3 + hashSuffix.length];
      hash[0] = 'm';
      hash[1] = 'd';
      hash[2] = '5';
      System.arraycopy(hashSuffix, 0, hash, 3, hashSuffix.length);
      return new PasswordMessage(hash);
    }

    static PasswordMessage clearText(String password) {
      try {
        return new PasswordMessage(Util.threadLocalStringEncoder.get().encode(CharBuffer.wrap(password)).array());
      } catch (CharacterCodingException e) { throw new IllegalArgumentException(e); }
    }

    final byte[] bytes;

    private PasswordMessage(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeByte((byte) 'p').lengthIntBegin().writeBytes(bytes).writeByte((byte) 0).lengthIntEnd();
    }
  }

  class Query implements FrontendMessage {
    final String query;

    Query(String query) { this.query = query; }

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeByte((byte) 'Q').lengthIntBegin().writeString(query).lengthIntEnd();
    }
  }

  class SslRequest implements FrontendMessage {
    static final SslRequest INSTANCE = new SslRequest();

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeInt(8).writeInt(80877103);
    }
  }

  class StartupMessage implements FrontendMessage {
    static final int DEFAULT_PROTOCOL = 196608;

    final int protocol;
    final Map<String, String> parameters;

    StartupMessage(Map<String, String> parameters) { this(DEFAULT_PROTOCOL, parameters); }

    public StartupMessage(int protocol, Map<String, String> parameters) {
      this.protocol = protocol;
      this.parameters = parameters;
    }

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.lengthIntBegin().writeInt(protocol);
      parameters.forEach((name, value) -> ctx.writeString(name).writeString(value));
      ctx.lengthIntEnd();
    }
  }

  class Sync implements FrontendMessage {
    static final Sync INSTANCE = new Sync();

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeByte((byte) 'S').lengthIntBegin().lengthIntEnd();
    }
  }

  class Terminate implements FrontendMessage {
    static final Terminate INSTANCE = new Terminate();

    @Override
    public void marshal(MarshalContext ctx) {
      ctx.writeByte((byte) 'X').lengthIntBegin().lengthIntEnd();
    }
  }

  class MarshalContext {
    private ByteBuffer buf;
    private boolean directBuf = true;
    private int lastLengthBegin = -1;

    ByteBuffer ensureCapacity(int needed) {
      if (buf.remaining() >= needed) return buf;
      // Round up to 1000 and then add it
      int newAmount = (((buf.capacity() + needed) / 1000) + 1) * 1000;
      buf = (directBuf ? ByteBuffer.allocateDirect(newAmount) : ByteBuffer.allocate(newAmount)).put(buf);
      return buf;
    }

    MarshalContext lengthIntBegin() {
      if (lastLengthBegin != -1) throw new IllegalStateException("Length already started");
      lastLengthBegin = buf.position() + 4;
      return writeInt(0);
    }

    MarshalContext lengthIntEnd() {
      if (lastLengthBegin == -1) throw new IllegalStateException("Length not started");
      buf.putInt(lastLengthBegin, buf.position() - lastLengthBegin);
      lastLengthBegin = -1;
      return this;
    }

    MarshalContext writeByte(byte b) {
      ensureCapacity(1).put(b);
      return this;
    }

    MarshalContext writeBytes(byte[] b) {
      ensureCapacity(b.length).put(b);
      return this;
    }

    MarshalContext writeShort(short s) {
      ensureCapacity(2).putShort(s);
      return this;
    }

    MarshalContext writeInt(int i) {
      ensureCapacity(4).putInt(i);
      return this;
    }

    MarshalContext writeString(String s) {
      ByteBuffer strBuf;
      try {
        strBuf = Util.threadLocalStringEncoder.get().encode(CharBuffer.wrap(s));
      } catch (CharacterCodingException e) { throw new IllegalArgumentException(e); }
      ensureCapacity(strBuf.limit() + 1).put(strBuf).put((byte) 0);
      return this;
    }
  }
}
