package asyncpg;

import java.util.HashMap;
import java.util.Map;

public class ServerException extends RuntimeException {
  protected static ServerException fromContext(ConnectionContext ctx) {
    Map<Byte, String> fields = new HashMap<>();
    while (true) {
      byte b = ctx.buf.get();
      if (b == 0) return new ServerException(fields);
      fields.put(b, ctx.bufReadString());
    }
  }

  public final Map<Byte, String> fields;

  protected ServerException(Map<Byte, String> fields) {
    super(fields.get((byte) 'M'));
    this.fields = fields;
  }
}
