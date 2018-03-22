package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

/** Reader that translates Postgres values to Java values */
public class RowReader {
  /** Read-only set of converters from {@link Converters#loadAllToConverters()} */
  public static final Map<String, Converters.To> DEFAULT_CONVERTERS =
      Collections.unmodifiableMap(Converters.loadAllToConverters());
  /** Singleton RowReader using only the {@link #DEFAULT_CONVERTERS} */
  public static final RowReader DEFAULT = new RowReader(DEFAULT_CONVERTERS, false);

  protected final Map<String, Converters.To> converters;

  /** Shortcut for {@link #RowReader(Map, boolean)} that does prepend defaults */
  public RowReader(Map<String, Converters.To> converterOverrides) {
    this(converterOverrides, true);
  }

  /**
   * Create a RowReader with the given converters. If prependDefaults is true, defaults are set first and the given
   * converters override them.
   */
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

  /** Shortcut for {@link #getRaw(QueryMessage.Row, int)} that requires row metadata for name-to-index lookup */
  public byte@Nullable [] getRaw(QueryMessage.Row row, String colName) {
    if (row.meta == null) throw new DriverException.MissingRowMeta();
    QueryMessage.RowMeta.Column col = row.meta.columnsByName.get(colName.toLowerCase());
    if (col == null) throw new DriverException.ColumnNotPresent("No column for name " + colName);
    return row.raw[col.index];
  }

  /**
   * Get the raw byte array from the given row for the column index. Shortcut to accessing {@link QueryMessage.Row#raw}
   * directly.
   */
  public byte@Nullable [] getRaw(QueryMessage.Row row, int colIndex) {
    if (colIndex < 0 || colIndex > row.raw.length)
      throw new DriverException.ColumnNotPresent("No column at index " + colIndex);
    return row.raw[colIndex];
  }

  /** Shortcut for {@link #get(QueryMessage.Row, int, Class)} that requires row metadata for name-to-index lookup */
  public <T> @Nullable T get(QueryMessage.Row row, String colName, Class<T> typ) {
    if (row.meta == null) throw new DriverException.MissingRowMeta();
    QueryMessage.RowMeta.Column col = row.meta.columnsByName.get(colName.toLowerCase());
    if (col == null) throw new DriverException.ColumnNotPresent("No column for name " + colName);
    return get(col, row.raw[col.index], typ);
  }

  /**
   * Get the row value from the column index and use the RowReader's converters to convert to the given type. This
   * defers to {@link #get(QueryMessage.RowMeta.Column, byte[], Class)}.
   */
  public <T> @Nullable T get(QueryMessage.Row row, int colIndex, Class<T> typ) {
    if (colIndex < 0 || colIndex > row.raw.length)
      throw new DriverException.ColumnNotPresent("No column at index " + colIndex);
    // No meta data means we use the unspecified type
    QueryMessage.RowMeta.Column col;
    if (row.meta != null) col = row.meta.columns[colIndex];
    else col = new QueryMessage.RowMeta.Column(colIndex, "", 0, (short) 0, DataType.UNSPECIFIED, (short) 0, 0, true);
    return get(col, row.raw[colIndex], typ);
  }

  @SuppressWarnings("unchecked")
  protected <T> Converters.@Nullable To<? extends T> getConverter(Class<T> typ) {
    if (typ.isPrimitive()) typ = Util.boxedClassFromPrimitive(typ);
    Converters.To conv = converters.get(typ.getName());
    // TODO: interfaces?
    if (conv != null || typ.getSuperclass() == null) return conv;
    return (Converters.To<? extends T>) getConverter(typ.getSuperclass());
  }

  /**
   * Get the column value and use the RowReader's converters to convert to the given type. If a converter is not found
   * for the exact class, its superclasses (not interfaces) are tried. If no converter is found, an exception is thrown.
   * Null is returned if the value is null. As a special case, if the type is an array and there are no converters for
   * it, Postgres array type is assumed and the component type is used. If the type is a map and there are no converters
   * for it, Postgres hstore type is assumed and a Map with keys and values as strings is returned.
   */
  @SuppressWarnings("unchecked")
  public <T> @Nullable T get(QueryMessage.RowMeta.Column col, byte@Nullable [] bytes, Class<T> typ) {
    Converters.To<? extends T> conv = getConverter(typ);
    if (conv == null) {
      // Handle as an array if necessary
      if (typ.isArray()) {
        try {
          return getArray(col, bytes, typ);
        } catch (Exception e) { throw new DriverException.ConvertToFailed(typ, col.dataTypeOid, e); }
      }
      if (Map.class.isAssignableFrom(typ)) {
        try {
          return (T) getHStore(col, bytes, String.class, String.class);
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

  // Creates a fake column of unspecified type
  /**
   * Convert the given Postgres string into the given type using
   * {@link #get(QueryMessage.RowMeta.Column, byte[], Class)} with a fake column set to text format.
   */
  public <T> @Nullable T get(@Nullable String val, Class<T> typ) {
    return get(new QueryMessage.RowMeta.Column(0, "", 0, (short) 0, DataType.UNSPECIFIED, (short) 0, 0, true),
        val == null ? null : Util.bytesFromString(val), typ);
  }

  @SuppressWarnings("unchecked")
  protected <T> @Nullable T getArray(QueryMessage.RowMeta.Column col, byte@Nullable [] bytes, Class<T> typ) {
    if (bytes == null) return null;
    Converters.BuiltIn.assertNotBinary(col.textFormat);
    char[] chars = Util.charsFromBytes(bytes);
    char delim = getArrayDelimiter(typ);
    StreamingTextContext ctx = new StreamingTextContext(chars, new char[] { delim, '}' });
    T ret = readArray(col, ctx, typ, delim);
    if (ctx.index != chars.length - 1) throw new IllegalArgumentException("Unexpected chars after array end");
    return ret;
  }

  @SuppressWarnings("unchecked")
  protected char getArrayDelimiter(Class typ) {
    Converters.To conv = getConverter(typ);
    if (conv != null) return conv.arrayDelimiter();
    if (typ.isArray()) return getArrayDelimiter(typ.getComponentType());
    return ',';
  }

  protected @Nullable String readCollectionItemText(StreamingTextContext ctx) {
    if (ctx.chars[ctx.index] == '"') {
      ctx.strBuf.setLength(0);
      ctx.index++;
      while (ctx.chars.length > ctx.index && ctx.chars[ctx.index] != '"') {
        if (ctx.chars[ctx.index] == '\\') {
          ctx.index++;
          if (ctx.chars.length <= ctx.index) break;
        }
        ctx.strBuf.append(ctx.chars[ctx.index]);
        ctx.index++;
      }
      if (ctx.chars.length <= ctx.index) throw new IllegalArgumentException("Unexpected end of quote string");
      ctx.index++;
      return ctx.strBuf.toString();
    }
    // Read until next end char
    int startIndex = ctx.index;
    while (ctx.chars.length > ctx.index && !ctx.isSubItemEndChar(ctx.chars[ctx.index])) ctx.index++;
    String str = new String(Arrays.copyOfRange(ctx.chars, startIndex, ctx.index));
    return str.equals("NULL") ? null : str;
  }

  @SuppressWarnings("unchecked")
  protected <T> T readArray(QueryMessage.RowMeta.Column col, StreamingTextContext ctx, Class<T> typ, char delim) {
    if (ctx.chars.length > ctx.index + 1 && ctx.chars[ctx.index] != '{')
      throw new IllegalArgumentException("Array must start with brace");
    ctx.index++;
    QueryMessage.RowMeta.Column subCol = col.child(DataType.arrayComponentOid(col.dataTypeOid));
    // TODO: what if we don't want sub-arrays to be array types...we don't want to look ahead for the end though
    Class subType;
    if (typ == Object.class) subType = Object.class;
    else if (typ.isArray()) subType = typ.getComponentType();
    else throw new IllegalArgumentException("Found sub array but expected type is not object or array type");
    List list = new ArrayList();
    while (ctx.chars.length > ctx.index && ctx.chars[ctx.index] != '}') {
      // If we're not the first, expect a delimiter
      if (!list.isEmpty()) {
        if (ctx.chars[ctx.index] != delim) throw new IllegalArgumentException("Missing delimiter");
        ctx.index++;
      }
      if (ctx.chars[ctx.index] == '{') {
        list.add(readArray(col.child(DataType.UNSPECIFIED), ctx, subType, delim));
        if (ctx.chars[ctx.index] != '}') throw new IllegalArgumentException("Unexpected array end");
        ctx.index++;
      } else {
        String str = readCollectionItemText(ctx);
        if (str == null) list.add(null);
        else list.add(get(subCol, Util.bytesFromString(str), subType));
      }
    }
    if (ctx.chars.length <= ctx.index) throw new IllegalArgumentException("Unexpected end");
    return (T) Util.listToArray(list, subType);
  }

  @SuppressWarnings("unchecked")
  protected <K, V> @Nullable Map<K, @Nullable V> getHStore(QueryMessage.RowMeta.Column col, byte@Nullable [] bytes,
      Class<K> keyTyp, Class<V> valTyp) {
    if (bytes == null) return null;
    Converters.BuiltIn.assertNotBinary(col.textFormat);
    char[] chars = Util.charsFromBytes(bytes);
    StreamingTextContext ctx = new StreamingTextContext(chars, new char[] { ',', '=' });
    return readHStore(col, ctx, keyTyp, valTyp);
  }

  @SuppressWarnings("unchecked")
  protected <K, V> Map<K, @Nullable V> readHStore(QueryMessage.RowMeta.Column col, StreamingTextContext ctx,
      Class<K> keyTyp, Class<V> valTyp) {
    Map<K, @Nullable V> map = new HashMap<>();
    QueryMessage.RowMeta.Column subCol = col.child(DataType.UNSPECIFIED);
    while (ctx.chars.length > ctx.index) {
      // If we're not the first, expect comma + space
      if (!map.isEmpty()) {
        if (ctx.index + 2 >= ctx.chars.length || ctx.chars[ctx.index] != ',' || ctx.chars[ctx.index + 1] != ' ')
          throw new IllegalArgumentException("Missing delimiter");
        ctx.index += 2;
      }
      // Read key
      String keyStr = readCollectionItemText(ctx);
      if (keyStr == null) throw new IllegalArgumentException("Unexpected null key");
      if (ctx.index + 2 >= ctx.chars.length || ctx.chars[ctx.index] != '=' || ctx.chars[ctx.index + 1] != '>')
        throw new IllegalArgumentException("Map key without value");
      ctx.index += 2;
      K key;
      if (keyTyp == String.class || keyTyp == Object.class) key = (K) keyStr;
      else key = get(subCol, Util.bytesFromString(keyStr), keyTyp);
      if (key == null) throw new IllegalArgumentException("Unexpected null key");
      // Read val
      String valStr = readCollectionItemText(ctx);
      V val;
      if (valStr == null) val = null;
      else if (valTyp == String.class || valTyp == Object.class) val = (V) valStr;
      else val = get(subCol, Util.bytesFromString(valStr), valTyp);
      map.put(key, val);
    }
    return map;
  }

  protected static class StreamingTextContext {
    public final char[] chars;
    public int index;
    public final StringBuilder strBuf = new StringBuilder();
    public final char[] subItemEndChars;

    public StreamingTextContext(char[] chars, char[] subItemEndChars) {
      this.chars = chars;
      this.subItemEndChars = subItemEndChars;
    }

    public boolean isSubItemEndChar(char chr) {
      for (char subItemEndChar : subItemEndChars) if (subItemEndChar == chr) return true;
      return false;
    }
  }
}
