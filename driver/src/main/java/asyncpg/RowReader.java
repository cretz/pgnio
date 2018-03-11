package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static asyncpg.DataType.*;

public class RowReader {

  public static final RowReader DEFAULT;

  // Keyed by class name
  public static final Map<String, Converter> DEFAULT_CONVERTERS;

  static {
    Map<String, Converter> def = new HashMap<>();
    def.put(Integer.class.getName(), RowReader::convertInteger);
    def.put(Short.class.getName(), RowReader::convertShort);
    def.put(String.class.getName(), RowReader::convertString);
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
    T ret;
    try {
      ret = (T) conv.convertNullable(DataType.UNSPECIFIED, true, row.raw[colIndex]);
    } catch (Exception e) { throw new DriverException.ConversionFailed(typ, DataType.UNSPECIFIED, e); }
    if (ret == null) throw new DriverException.InvalidConvertDataType(typ, DataType.UNSPECIFIED);
    return ret;
  }

  @SuppressWarnings("unchecked")
  public <T> T get(QueryMessage.RowMeta.Column col, byte@Nullable [] bytes, Class<T> typ) {
    Converter conv = converters.get(typ.getName());
    if (conv == null) throw new DriverException.NoConversion(typ);
    T ret;
    try {
      ret = (T) conv.convertNullable(col, bytes);
    } catch (Exception e) { throw new DriverException.ConversionFailed(typ, col.dataTypeOid, e); }
    if (ret == null) throw new DriverException.InvalidConvertDataType(typ, col.dataTypeOid);
    return ret;
  }

  protected static void assertNotBinary(boolean formatText) {
    if (!formatText) throw new UnsupportedOperationException("Binary not supported yet");
  }

  protected static @Nullable Integer convertInteger(
      int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
    assertNotBinary(formatText);
    switch (dataTypeOid) {
      case UNSPECIFIED:
      case INT2:
      case INT4:
        return Integer.valueOf(convertString(bytes));
      default: return null;
    }
  }

  protected static @Nullable Short convertShort(int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
    assertNotBinary(formatText);
    switch (dataTypeOid) {
      case UNSPECIFIED:
      case INT2:
        return Short.valueOf(convertString(bytes));
      default: return null;
    }
  }

  protected static String convertString(byte[] bytes) throws Exception {
    return Util.threadLocalStringDecoder.get().decode(ByteBuffer.wrap(bytes)).toString();
  }

  protected static @Nullable String convertString(int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
    assertNotBinary(formatText);
    switch (dataTypeOid) {
      case UNSPECIFIED:
      case VARCHAR:
        return convertString(bytes);
      default: return null;
    }
  }

  @FunctionalInterface
  interface Converter<T> {
    default @Nullable T convertNullable(QueryMessage.RowMeta.Column column, byte@Nullable [] bytes) throws Exception {
      return convertNullable(column.dataTypeOid, column.formatText, bytes);
    }

    default @Nullable T convertNullable(int dataTypeOid, boolean formatText, byte@Nullable [] bytes) throws Exception {
      return bytes == null ? null : convert(dataTypeOid, formatText, bytes);
    }

    // If this returns null, it is assumed this cannot decode it
    @Nullable T convert(int dataTypeOid, boolean formatText, byte[] bytes) throws Exception;
  }
}
