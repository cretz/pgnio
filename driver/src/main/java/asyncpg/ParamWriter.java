package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Converter from Java types to Postgres values */
public class ParamWriter {
  /** Read only from-converter set via {@link Converters#loadAllFromConverters()} keyed by class name */
  public static final Map<String, Converters.From> DEFAULT_CONVERTERS =
      Collections.unmodifiableMap(Converters.loadAllFromConverters());
  /** Singleton ParamWriter holding the {@link #DEFAULT_CONVERTERS} */
  public static final ParamWriter DEFAULT = new ParamWriter(DEFAULT_CONVERTERS, false);

  protected final Map<String, Converters.From> converters;

  /** Shortcut for {@link #ParamWriter(Map, boolean)} that prepends defaults */
  public ParamWriter(Map<String, Converters.From> converterOverrides) {
    this(converterOverrides, true);
  }

  /**
   * Create a new param writer with teh given converters. If prependDefaults is true, those are added before they are
   * overridden with the given converters.
   */
  public ParamWriter(Map<String, Converters.From> converters, boolean prependDefaults) {
    Map<String, Converters.From> map;
    if (prependDefaults) {
      map = new HashMap<>(DEFAULT_CONVERTERS.size() + converters.size());
      map.putAll(DEFAULT_CONVERTERS);
    } else {
      map = new HashMap<>(converters.size());
    }
    map.putAll(converters);
    this.converters = Collections.unmodifiableMap(map);
  }

  protected <@Nullable T> Converters.@Nullable From<? extends T> getConverter(Class<T> typ) {
    return getConverter(typ, true);
  }

  @SuppressWarnings("unchecked")
  protected <@Nullable T> Converters.@Nullable From<? extends T> getConverter(Class<T> typ, boolean topLevel) {
    Converters.From conv = converters.get(typ.getName());
    if (conv == null) {
      if (typ.isArray()) {
        Converters.From subConv = getConverter(typ.getComponentType(), false);
        if (subConv != null) conv = new ArrayConverter(topLevel, subConv.arrayDelimiter());
      } else if (Map.class.isAssignableFrom(typ)) {
        conv = new HStoreConverter(topLevel);
      }
    }
    if (conv != null || typ.getSuperclass() == null) return conv;
    return (Converters.From<? extends T>) getConverter(typ.getSuperclass(), topLevel);
  }

  /** Shortcut for {@link #write(boolean, Object, BufWriter, boolean)} with asSql as false */
  public void write(boolean textFormat, Object obj, BufWriter buf) {
    write(textFormat, obj, buf, false);
  }

  /**
   * Convert the given obj to buf. If asSql is true, textFormat must be true and it is written as though it is a SQL
   * string (e.g. quoted with string escaping).
   */
  @SuppressWarnings("unchecked")
  public void write(boolean textFormat, Object obj, BufWriter buf, boolean asSql) {
    if (asSql) Converters.BuiltIn.assertNotBinary(textFormat);
    // We don't look up the class list here, we expect the map to have all exact instances
    Converters.From conv = getConverter(obj.getClass());
    if (conv == null) throw new DriverException.NoConversion(obj.getClass());
    try {
      boolean needsQuote = asSql && conv.mustBeQuotedWhenUsedInSql(obj);
      if (needsQuote) buf.writeByte((byte) '\'').writeStringEscapeSingleQuoteBegin();
      try {
        conv.convertFrom(textFormat, obj, buf);
      } finally { if (needsQuote) buf.writeStringEscapeSingleQuoteEnd(); }
      if (needsQuote) buf.writeByte((byte) '\'');
    } catch (Exception e) { throw new DriverException.ConvertFromFailed(obj.getClass(), e); }
  }

  @SuppressWarnings("unchecked")
  protected void writeCollectionItemText(@Nullable Object obj, BufWriter buf) {
    if (obj == null) {
      buf.writeString("NULL");
      return;
    }
    Converters.From conv = getConverter(obj.getClass(), false);
    if (conv == null) throw new DriverException.NoConversion(obj.getClass());
    boolean needsQuote = conv.mustBeQuotedWhenUsedInSql(obj);
    if (needsQuote) buf.writeString("\"").writeStringEscapeDoubleQuoteBegin();
    try {
      conv.convertFrom(true, obj, buf);
    } finally {
      if (needsQuote) buf.writeStringEscapeDoubleQuoteEnd();
    }
    if (needsQuote) buf.writeString("\"");
  }

  /** Converter to write Postgres arrays from Java arrays */
  public class ArrayConverter implements Converters.From {
    protected final boolean topLevel;
    protected final char arrayDelimiter;

    public ArrayConverter(boolean topLevel, char arrayDelimiter) {
      this.topLevel = topLevel;
      this.arrayDelimiter = arrayDelimiter;
    }

    @Override
    public char arrayDelimiter() { return arrayDelimiter; }

    @Override
    public boolean mustBeQuotedWhenUsedInSql(Object obj) { return topLevel; }

    @Override
    public void convertFrom(boolean textFormat, Object obj, BufWriter buf) {
      Converters.BuiltIn.assertNotBinary(textFormat);
      buf.writeByte((byte) '{');
      int length = Array.getLength(obj);
      for (int i = 0; i < length; i++) {
        if (i > 0) buf.writeByte((byte) arrayDelimiter);
        writeCollectionItemText(Array.get(obj, i), buf);
      }
      buf.writeByte((byte) '}');
    }
  }

  /** Converter to write Postgres hstores from Java maps */
  public class HStoreConverter implements Converters.From<Map<?, ?>> {
    protected final boolean topLevel;

    public HStoreConverter(boolean topLevel) { this.topLevel = topLevel; }

    @Override
    public boolean mustBeQuotedWhenUsedInSql(Map obj) { return true; }

    @Override
    public void convertFrom(boolean textFormat, Map<?, ?> obj, BufWriter buf) {
      Converters.BuiltIn.assertNotBinary(textFormat);
      boolean first = true;
      for (Map.Entry<?, ?> entry : obj.entrySet()) {
        if (first) first = false;
        else buf.writeByte((byte) ',');
        writeCollectionItemText(entry.getKey(), buf);
        buf.writeString("=>");
        writeCollectionItemText(entry.getValue(), buf);
      }
    }
  }
}
