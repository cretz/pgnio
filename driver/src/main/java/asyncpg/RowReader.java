package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RowReader {
  public static final Map<String, Converters.To> DEFAULT_CONVERTERS =
      Collections.unmodifiableMap(Converters.loadAllToConverters());
  public static final RowReader DEFAULT = new RowReader(DEFAULT_CONVERTERS, false);

  protected final Map<String, Converters.To> converters;

  public RowReader(Map<String, Converters.To> converterOverrides) {
    this(converterOverrides, true);
  }

  public RowReader(Map<String, Converters.To> converters, boolean prependDefaults) {
    Map<String, Converters.To> map;
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
    Converters.To conv = converters.get(typ.getName());
    if (conv == null) throw new DriverException.NoConversion(typ);
    byte[] bytes = row.raw[colIndex];
    T ret;
    try {
      ret = (T) conv.convertToNullable(DataType.UNSPECIFIED, true, bytes);
    } catch (Exception e) { throw new DriverException.ConvertToFailed(typ, DataType.UNSPECIFIED, e); }
    if (bytes != null && ret == null) throw new DriverException.InvalidConvertDataType(typ, DataType.UNSPECIFIED);
    return ret;
  }

  @SuppressWarnings("unchecked")
  public <T> T get(QueryMessage.RowMeta.Column col, byte@Nullable [] bytes, Class<T> typ) {
    Converters.To conv = converters.get(typ.getName());
    if (conv == null) throw new DriverException.NoConversion(typ);
    T ret;
    try {
      ret = (T) conv.convertToNullable(col, bytes);
    } catch (Exception e) { throw new DriverException.ConvertToFailed(typ, col.dataTypeOid, e); }
    if (bytes != null && ret == null) throw new DriverException.InvalidConvertDataType(typ, col.dataTypeOid);
    return ret;
  }
}
