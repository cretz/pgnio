package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.CharBuffer;
import java.util.*;

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

  public byte@Nullable [] getRaw(QueryMessage.Row row, String colName) {
    if (row.meta == null) throw new DriverException.MissingRowMeta();
    QueryMessage.RowMeta.Column col = row.meta.columnsByName.get(colName.toLowerCase());
    if (col == null) throw new DriverException.ColumnNotPresent("No column for name " + colName);
    return row.raw[col.index];
  }

  public byte@Nullable [] getRaw(QueryMessage.Row row, int colIndex) {
    if (colIndex < 0 || colIndex > row.raw.length)
      throw new DriverException.ColumnNotPresent("No column at index " + colIndex);
    return row.raw[colIndex];
  }

  public <@Nullable T> T get(QueryMessage.Row row, String colName, Class<T> typ) {
    if (row.meta == null) throw new DriverException.MissingRowMeta();
    QueryMessage.RowMeta.Column col = row.meta.columnsByName.get(colName.toLowerCase());
    if (col == null) throw new DriverException.ColumnNotPresent("No column for name " + colName);
    return get(col, row.raw[col.index], typ);
  }

  public <@Nullable T> T get(QueryMessage.Row row, int colIndex, Class<T> typ) {
    if (colIndex < 0 || colIndex > row.raw.length)
      throw new DriverException.ColumnNotPresent("No column at index " + colIndex);
    // No meta data means we use the unspecified type
    QueryMessage.RowMeta.Column col;
    if (row.meta != null) col = row.meta.columns[colIndex];
    else col = new QueryMessage.RowMeta.Column(colIndex, "", 0, (short) 0, DataType.UNSPECIFIED, (short) 0, 0, true);
    return get(col, row.raw[colIndex], typ);
  }

  @SuppressWarnings("unchecked")
  protected <@Nullable T> Converters.@Nullable To<? extends T> getConverter(Class<T> typ) {
    Converters.To conv = converters.get(typ.getName());
    if (conv != null || typ.getSuperclass() == null) return conv;
    return (Converters.To<? extends T>) getConverter(typ.getSuperclass());
  }

  @SuppressWarnings("unchecked")
  public <@Nullable T> T get(QueryMessage.RowMeta.Column col, byte@Nullable [] bytes, Class<T> typ) {
    Converters.To<? extends T> conv = getConverter(typ);
    if (conv == null) {
      // Handle as an array if necessary
      if (typ.isArray()) {
        if (bytes == null) return null;
        try {
          return getArray(col, bytes, typ);
        } catch (Exception e) { throw new DriverException.ConvertToFailed(typ, col.dataTypeOid, e); }
      }
      throw new DriverException.NoConversion(typ);
    }
    T ret;
    try {
      ret = conv.convertToNullable(col, bytes);
    } catch (Exception e) { throw new DriverException.ConvertToFailed(typ, col.dataTypeOid, e); }
    if (bytes != null && ret == null) throw new DriverException.InvalidConvertDataType(typ, col.dataTypeOid);
    return ret;
  }

  @SuppressWarnings("unchecked")
  protected <@Nullable T> T getArray(QueryMessage.RowMeta.Column col, byte[] bytes, Class<T> typ) {
    Converters.BuiltIn.assertNotBinary(col.textFormat);
    List<T> ret = new ArrayList<>();
    char[] chars = Util.charsFromBytes(bytes);
    int index = readArray(col, chars, 0, ret, typ, getArrayDelimiter(typ));
    if (index != chars.length - 1) throw new IllegalArgumentException("Unexpected chars after array end");
    return (T) ret.toArray();
  }

  @SuppressWarnings("unchecked")
  protected char getArrayDelimiter(Class typ) {
    Converters.To conv = getConverter(typ);
    if (conv != null) return conv.arrayDelimiter();
    if (typ.isArray()) return getArrayDelimiter(typ.getComponentType());
    return ',';
  }

  @SuppressWarnings("unchecked")
  protected <@Nullable T> int readArray(QueryMessage.RowMeta.Column col, char[] chars,
      int index, List<T> list, Class<T> typ, char delim) {
    if (chars.length > index + 1 && chars[index] != '{')
      throw new IllegalArgumentException("Array must start with brace");
    StringBuilder strBuf = new StringBuilder();
    index++;
    QueryMessage.RowMeta.Column subCol = col.child(DataType.arrayComponentOid(col.dataTypeOid));
    // TODO: what if we don't want sub-arrays to be array types...we don't want to look ahead for the end though
    Class subType;
    if (typ == Object.class) subType = Object.class;
    else if (typ.isArray()) subType = typ.getComponentType();
    else throw new IllegalArgumentException("Found sub array but expected type is not object or array type");
    while (chars.length > index && chars[index] != '}') {
      // If we're not the first, expect a comma
      if (!list.isEmpty()) {
        if (chars[index] != delim) throw new IllegalArgumentException("Missing delimiter");
        index++;
      }
      // Check null, or quoted string, or sub array, or just value
      if (chars[index] == 'N' && chars.length > index + 4 && chars[index + 1] == 'U' &&
          chars[index + 2] == 'L' && chars[index + 3] == 'L' &&
          (chars[index + 4] == delim || chars[index + 4] == '}' || Character.isWhitespace(chars[index + 4]))) {
        list.add(null);
        index += 4;
      } else if (chars[index] == '"') {
        strBuf.setLength(0);
        index++;
        while (chars.length > index && chars[index] != '"') {
          if (chars[index] == '\\') {
            index++;
            if (chars.length <= index) break;
          }
          strBuf.append(chars[index]);
          index++;
        }
        if (chars.length <= index) throw new IllegalArgumentException("Unexpected end of quote string");
        list.add((T) get(subCol, Util.bytesFromCharBuffer(CharBuffer.wrap(strBuf)), subType));
        index++;
      } else if (chars[index] == '{') {
        List subList = new ArrayList();
        index = readArray(col.child(DataType.UNSPECIFIED), chars, index, subList, subType, delim);
        if (chars[index] != '}') throw new IllegalArgumentException("Unexpected array end");
        index++;
        list.add((T) subList.toArray());
      } else {
        // // Just run until the next delim or end brace
        int startIndex = index;
        while (chars.length > index && chars[index] != delim && chars[index] != '}') index++;
        if (chars.length <= index) throw new IllegalArgumentException("Unexpected value end");
        char[] subChars = Arrays.copyOfRange(chars, startIndex, index);
        list.add((T) get(subCol, Util.bytesFromCharBuffer(CharBuffer.wrap(subChars)), subType));
      }
    }
    if (chars.length <= index) throw new IllegalArgumentException("Unexpected end");
    return index;
  }
}
