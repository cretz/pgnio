package asyncpg;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static asyncpg.DataType.UNSPECIFIED;
import static asyncpg.DataType.VARCHAR;
import static asyncpg.DataType.nameForOid;

public class RowReader {

  public static final RowReader DEFAULT;

  // Keyed by class name
  public static final Map<String, BiFunction<QueryMessage.RowMeta.Column, byte[], ?>> DEFAULT_CONVERTERS;

  static {
    Map<String, BiFunction<QueryMessage.RowMeta.Column, byte[], ?>> def = new HashMap<>();
    // BiFunction<QueryMessage.RowMeta.Column, byte[], ?> temp;
    def.put(String.class.getName(), RowReader::convertString);
    DEFAULT_CONVERTERS = Collections.unmodifiableMap(def);
    DEFAULT = new RowReader(def, false);
  }

  protected final Map<String, BiFunction<QueryMessage.RowMeta.Column, byte[], ?>> converters;

  public RowReader(Map<String, BiFunction<QueryMessage.RowMeta.Column, byte[], ?>> converterOverrides) {
    this(converterOverrides, true);
  }

  public RowReader(Map<String, BiFunction<QueryMessage.RowMeta.Column, byte[], ?>> converters,
      boolean prefixDefaults) {
    Map<String, BiFunction<QueryMessage.RowMeta.Column, byte[], ?>> map;
    if (prefixDefaults) {
      map = new HashMap<>(DEFAULT_CONVERTERS.size() + converters.size());
      map.putAll(DEFAULT_CONVERTERS);
    } else {
      map = new HashMap<>(converters.size());
    }
    map.putAll(converters);
    this.converters = Collections.unmodifiableMap(map);
  }

  public <T> T get(QueryMessage.Row row, String colName, Class<T> typ) {
    QueryMessage.RowMeta.Column col = row.meta.columnsByName.get(colName.toLowerCase());
    if (col == null) throw new ColumnNotPresentException("No column for name " + colName);
    return get(row, col, row.raw[col.index], typ);
  }

  public <T> T get(QueryMessage.Row row, int colIndex, Class<T> typ) {
    if (colIndex < 0 || colIndex > row.raw.length)
      throw new ColumnNotPresentException("No column at index " + colIndex);
    return get(row, row.meta.columns[colIndex], row.raw[colIndex], typ);
  }

  public <T> T get(QueryMessage.Row row, QueryMessage.RowMeta.Column col, byte[] bytes, Class<T> typ) {
    BiFunction<QueryMessage.RowMeta.Column, byte[], ?> fn = converters.get(typ.getName());
    if (fn == null) throw new NoConversionException(typ);
    @SuppressWarnings("unchecked")
    T ret = (T) fn.apply(col, bytes);
    return ret;
  }

  protected static String convertString(QueryMessage.RowMeta.Column col, byte[] bytes) {
    switch (col.dataTypeOid) {
      case UNSPECIFIED:
      case VARCHAR:
        try {
          return Util.threadLocalStringDecoder.get().decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) { throw new ConversionFailedException(e); }
      default: throw new InvalidDataTypeException(String.class, col.dataTypeOid);
    }
  }

  public static class ColumnNotPresentException extends DriverException {
    public ColumnNotPresentException(String message) { super(message); }
  }

  public static class NoConversionException extends DriverException {
    public NoConversionException(Class cls) { super("No conversion defined for " + cls); }
  }

  public static class InvalidDataTypeException extends DriverException {
    protected static String oidToString(int oid) {
      String name = nameForOid(oid);
      return name == null ? String.valueOf(oid) : name;
    }

    public InvalidDataTypeException(Class cls, int oid) { super("Cannot convert " + cls + " to " + oidToString(oid)); }
  }

  public static class ConversionFailedException extends DriverException {
    public ConversionFailedException(String message) { super(message); }
    public ConversionFailedException(String message, Throwable cause) { super(message, cause); }
    public ConversionFailedException(Throwable cause) { super(cause); }
  }
}
