package asyncpg.driver;

import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.util.HashMap;
import java.util.Map;

interface BackendMessage {

  static BackendMessage fromIdentifier(byte b) {
    switch ((char) b) {
      case '1': return ParseComplete.INSTANCE;
      case '2': return BindComplete.INSTANCE;
      case '3': return CloseComplete.INSTANCE;
      case 'c': return CopyDone.INSTANCE;
      case 'd': return new CopyData();
      case 'n': return NoData.INSTANCE;
      case 's': return PortalSuspended.INSTANCE;
      case 't': return new ParameterDescription();
      case 'A': return new NotificationResponse();
      case 'C': return new CommandComplete();
      case 'D': return new DataRow();
      case 'E': return new ErrorResponse();
      case 'G': return new CopyInResponse();
      case 'H': return new CopyOutResponse();
      case 'K': return new BackendKeyData();
      case 'I': return EmptyQueryResponse.INSTANCE;
      case 'N': return new NoticeResponse();
      case 'R': return new Authentication();
      case 'S': return new ParameterStatus();
      case 'T': return new RowDescription();
      case 'W': return new CopyBothResponse();
      case 'Z': return new ReadyForQuery();
      default: throw new IllegalArgumentException("Unrecognized message type: " + ((char) b));
    }
  }

  void unmarshal(UnmarshalContext ctx);

  class Authentication implements BackendMessage {
    static final int TYPE_OK = 0;
    static final int TYPE_CLEARTEXT_PASSWORD = 3;
    static final int TYPE_MD5_PASSWORD = 5;
    int type;
    byte[] md5Salt;

    @Override
    public void unmarshal(UnmarshalContext ctx) {
      type = ctx.readInt();
      switch (type) {
        case TYPE_OK:
        case TYPE_CLEARTEXT_PASSWORD:
          return;
        case TYPE_MD5_PASSWORD:
          md5Salt = new byte[4];
          ctx.readBytes(md5Salt);
          return;
        default: throw new UnsupportedOperationException("Unrecognized auth type: " + type);
      }
    }
  }

  class BackendKeyData implements BackendMessage {
    int processId;
    int secretKey;

    @Override
    public void unmarshal(UnmarshalContext ctx) {
      processId = ctx.readInt();
      secretKey = ctx.readInt();
    }
  }

  class BindComplete implements BackendMessage {
    static final BindComplete INSTANCE = new BindComplete();

    @Override
    public void unmarshal(UnmarshalContext ctx) { }
  }

  class CloseComplete implements BackendMessage {
    static final CloseComplete INSTANCE = new CloseComplete();

    @Override
    public void unmarshal(UnmarshalContext ctx) { }
  }

  class CommandComplete implements BackendMessage {
    TagType tagType;
    long insertedOid;
    long rowCount;

    @Override
    public void unmarshal(UnmarshalContext ctx) {
      String tag = ctx.readString();
      int spaceIndex = tag.indexOf(' ');
      // XXX: we don't support COPY on PG < 8.2, so we can assume all have row count
      if (spaceIndex == -1) throw new IllegalStateException("Row count not present for command complete");
      tagType = TagType.valueOf(tag.substring(0, spaceIndex));
      tag = tag.substring(spaceIndex + 1);
      if (tagType == TagType.INSERT) {
        spaceIndex = tag.indexOf(' ');
        if (spaceIndex == -1) throw new IllegalStateException("Insert oid not present for command complete");
        insertedOid = Long.parseLong(tag.substring(0, spaceIndex));
        rowCount = Long.parseLong(tag.substring(spaceIndex + 1));
      } else {
        rowCount = Long.parseLong(tag);
      }
    }

    enum TagType { INSERT, DELETE, UPDATE, SELECT, MOVE, FETCH, COPY }
  }

  class CopyBothResponse extends CopyResponse {
  }

  class CopyData implements BackendMessage {
    byte[] bytes;

    @Override
    public void unmarshal(UnmarshalContext ctx) {
      bytes = ctx.readToEnd();
    }
  }

  class CopyDone implements BackendMessage {
    static final CopyDone INSTANCE = new CopyDone();

    @Override
    public void unmarshal(UnmarshalContext ctx) { }
  }

  class CopyInResponse extends CopyResponse {
  }

  class CopyOutResponse extends CopyResponse {
  }

  abstract class CopyResponse implements BackendMessage {
    boolean formatText;
    boolean[] columnsFormatText;

    @Override
    public void unmarshal(UnmarshalContext ctx) {
      formatText = ctx.readByte() == 0;
      columnsFormatText = new boolean[ctx.readShort()];
      for (int i = 0; i < columnsFormatText.length; i++) columnsFormatText[i] = ctx.readShort() == 0;
    }
  }

  class DataRow implements BackendMessage {
    byte[][] values;

    @Override
    public void unmarshal(UnmarshalContext ctx) {
      values = new byte[ctx.readShort()][];
      for (int i = 0; i < values.length; i++) {
        int length = ctx.readInt();
        if (length == -1) values[i] = null;
        else if (length == 0) values[i] = new byte[0];
        else {
          byte[] bytes = new byte[length];
          ctx.readBytes(bytes);
          values[i] = bytes;
        }
      }
    }
  }

  class EmptyQueryResponse implements BackendMessage {
    static final EmptyQueryResponse INSTANCE = new EmptyQueryResponse();

    @Override
    public void unmarshal(UnmarshalContext ctx) { }
  }

  class ErrorResponse extends StringFieldsResponse {
  }

  class NoData implements BackendMessage {
    static final NoData INSTANCE = new NoData();

    @Override
    public void unmarshal(UnmarshalContext ctx) { }
  }

  class NoticeResponse extends StringFieldsResponse {
  }

  class NotificationResponse implements BackendMessage {
    int processId;
    String channel;
    String payload;

    @Override
    public void unmarshal(UnmarshalContext ctx) {
      processId = ctx.readInt();
      channel = ctx.readString();
      payload = ctx.readString();
    }
  }

  class ParameterDescription implements BackendMessage {
    int[] parameterDataTypeOids;

    @Override
    public void unmarshal(UnmarshalContext ctx) {
      parameterDataTypeOids = new int[ctx.readShort()];
      for (int i = 0; i < parameterDataTypeOids.length; i++) parameterDataTypeOids[i] = ctx.readInt();
    }
  }

  class ParameterStatus implements BackendMessage {
    String name;
    String value;

    @Override
    public void unmarshal(UnmarshalContext ctx) {
      name = ctx.readString();
      value = ctx.readString();
    }
  }

  class ParseComplete implements BackendMessage {
    static final ParseComplete INSTANCE = new ParseComplete();
    @Override
    public void unmarshal(UnmarshalContext ctx) { }
  }

  class PortalSuspended implements BackendMessage {
    static final PortalSuspended INSTANCE = new PortalSuspended();

    @Override
    public void unmarshal(UnmarshalContext ctx) { }
  }

  class ReadyForQuery implements BackendMessage {
    TransactionStatus status;

    @Override
    public void unmarshal(UnmarshalContext ctx) {
      char s = (char) ctx.readByte();
      switch (s) {
        case 'I':
          status = TransactionStatus.IDLE;
          break;
        case 'T':
          status = TransactionStatus.IN_BLOCK;
          break;
        case 'E':
          status = TransactionStatus.FAILED_BLOCK;
          break;
        default:
          throw new IllegalArgumentException("Unknown transaction status: " + s);
      }
    }

    enum TransactionStatus { IDLE, IN_BLOCK, FAILED_BLOCK }
  }

  class RowDescription implements BackendMessage {
    Field[] fields;

    @Override
    public void unmarshal(UnmarshalContext ctx) {
      fields = new Field[ctx.readShort()];
      for (int i = 0; i < fields.length; i++) fields[i] = new Field(ctx);
    }

    static class Field {
      final String name;
      final int tableOid;
      final short columnAttributeNumber;
      final int dataTypeOid;
      final short dataTypeSize;
      final boolean dataTypeSizeVariable;
      final int typeModifier;
      final boolean formatText;

      Field(UnmarshalContext ctx) {
        name = ctx.readString();
        tableOid = ctx.readInt();
        columnAttributeNumber = ctx.readShort();
        dataTypeOid = ctx.readInt();
        short size = ctx.readShort();
        dataTypeSizeVariable = size < 0;
        dataTypeSize = dataTypeSizeVariable ? (short) -size : size;
        typeModifier = ctx.readInt();
        formatText = ctx.readShort() == 0;
      }
    }
  }

  abstract class StringFieldsResponse implements BackendMessage {
    final Map<Byte, String> fields = new HashMap<>();

    @Override
    public void unmarshal(UnmarshalContext ctx) {
      while (ctx.peekByte() != 0) fields.put(ctx.readByte(), ctx.readString());
      ctx.readByte();
    }
  }

  class UnmarshalContext {
    private final ByteBuffer buf;

    UnmarshalContext(ByteBuffer buf) { this.buf = buf; }

    byte peekByte() { return buf.get(buf.position()); }
    byte readByte() { return buf.get(); }
    void readBytes(byte[] b) { buf.get(b); }
    int readInt() { return buf.getInt(); }
    short readShort() { return buf.getShort(); }

    String readString() {
      int indexOfZero = buf.position();
      while (buf.get(indexOfZero) != 0) indexOfZero++;
      ByteBuffer strBuf = buf.slice();
      buf.limit(indexOfZero - buf.position());
      buf.position(indexOfZero + 1);
      try {
        return Util.threadLocalStringDecoder.get().decode(strBuf).toString();
      } catch (CharacterCodingException e) { throw new IllegalStateException(e); }
    }

    byte[] readToEnd() {
      byte[] ret = new byte[buf.remaining()];
      buf.get(ret);
      return ret;
    }
  }
}
