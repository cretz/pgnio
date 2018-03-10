package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static asyncpg.DataType.UNSPECIFIED;
import static asyncpg.DataType.VARCHAR;

public class RowReader {

  public static final RowReader DEFAULT;

  // Keyed by class name
  public static final Map<String, Converter> DEFAULT_CONVERTERS;

  static {
    Map<String, Converter> def = new HashMap<>();
    def.put(String.class.getName(), RowReader::convertString);
    // TODO: add more converters to def
    DEFAULT_CONVERTERS = Collections.unmodifiableMap(def);
    DEFAULT = new RowReader(def, false);
  }

  protected final Map<String, Converter> converters;

  public RowReader(Map<String, Converter> converterOverrides) {
    this(converterOverrides, true);
  }

  public RowReader(Map<String, Converter> converters, boolean prependDefaults) {
    Map<String, Converter> map;
    if (prependDefaults) {
      map = new HashMap<>(DEFAULT_CONVERTERS.size() + converters.size());
      map.putAll(DEFAULT_CONVERTERS);
    } else {
      map = new HashMap<>(converters.size());
    }
    map.putAll(converters);
    this.converters = Collections.unmodifiableMap(map);
  }

  public <T> T get(QueryMessage.Row row, String colName, Class<T> typ) {
    if (row.meta == null) throw new DriverException.MissingRowMeta();
    QueryMessage.RowMeta.Column col = row.meta.columnsByName.get(colName.toLowerCase());
    if (col == null) throw new DriverException.ColumnNotPresent("No column for name " + colName);
    return get(col, row.raw[col.index], typ);
  }

  @SuppressWarnings("unchecked")
  public <T> T get(QueryMessage.Row row, int colIndex, Class<T> typ) {
    if (colIndex < 0 || colIndex > row.raw.length)
      throw new DriverException.ColumnNotPresent("No column at index " + colIndex);
    if (row.meta != null) return get(row.meta.columns[colIndex], row.raw[colIndex], typ);
    // No meta data means we use the unspecified type
    Converter conv = converters.get(typ.getName());
    if (conv == null) throw new DriverException.NoConversion(typ);
    return (T) conv.convertNullable(DataType.UNSPECIFIED, row.raw[colIndex]);
  }

  @SuppressWarnings("unchecked")
  public <T> T get(QueryMessage.RowMeta.Column col, byte@Nullable [] bytes, Class<T> typ) {
    Converter conv = converters.get(typ.getName());
    if (conv == null) throw new DriverException.NoConversion(typ);
    return (T) conv.convertNullable(col, bytes);
  }

  protected static String convertString(int dataTypeOid, byte[] bytes) {
    switch (dataTypeOid) {
      case UNSPECIFIED:
      case VARCHAR:
        try {
          return Util.threadLocalStringDecoder.get().decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) { throw new ConversionFailedException(e); }
      default: throw new DriverException.InvalidConvertDataType(String.class, dataTypeOid);
    }
  }

  @FunctionalInterface
  interface Converter<T> {
    default @Nullable T convertNullable(QueryMessage.RowMeta.Column column, byte@Nullable [] bytes) {
      return convertNullable(column.dataTypeOid, bytes);
    }

    default @Nullable T convertNullable(int dataTypeOid, byte@Nullable [] bytes) {
      return bytes == null ? null : convert(dataTypeOid, bytes);
    }

    T convert(int dataTypeOid, byte[] bytes);
  }

  public static class ConversionFailedException extends DriverException {
    public ConversionFailedException(String message) { super(message); }
    public ConversionFailedException(String message, Throwable cause) { super(message, cause); }
    public ConversionFailedException(Throwable cause) { super(cause); }
  }
}
