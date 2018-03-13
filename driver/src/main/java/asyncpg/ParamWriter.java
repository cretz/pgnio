package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ParamWriter {

  // Keyed by class name
  public static final Map<String, Converters.From> DEFAULT_CONVERTERS =
      Collections.unmodifiableMap(Converters.loadAllFromConverters());
  public static final ParamWriter DEFAULT = new ParamWriter(DEFAULT_CONVERTERS, false);

  protected final Map<String, Converters.From> converters;

  public ParamWriter(Map<String, Converters.From> converterOverrides) {
    this(converterOverrides, true);
  }

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
    if (conv == null && typ.isArray()) {
      Converters.From subConv = getConverter(typ.getComponentType(), false);
      if (subConv != null) conv = new ArrayConverter(subConv, topLevel);
    }
    if (conv != null || typ.getSuperclass() == null) return conv;
    return (Converters.From<? extends T>) getConverter(typ.getSuperclass(), topLevel);
  }

  public void write(boolean textFormat, Object obj, BufWriter buf) {
    write(textFormat, obj, buf, false);
  }

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

  public static class ArrayConverter implements Converters.From {
    protected final Converters.From componentConverter;
    protected final boolean topLevel;

    public ArrayConverter(Converters.From componentConverter, boolean topLevel) {
      this.componentConverter = componentConverter;
      this.topLevel = topLevel;
    }

    @Override
    public boolean mustBeQuotedWhenUsedInSql(Object obj) { return topLevel; }

    @Override
    @SuppressWarnings("unchecked")
    public void convertFrom(boolean textFormat, Object obj, BufWriter buf) {
      Converters.BuiltIn.assertNotBinary(textFormat);
      buf.writeByte((byte) '{');
      int length = Array.getLength(obj);
      for (int i = 0; i < length; i++) {
        if (i > 0) buf.writeByte((byte) ',');
        Object subObj = Array.get(obj, i);
        if (subObj == null) {
          buf.writeString("NULL");
        } else {
          boolean needsQuote = componentConverter.mustBeQuotedWhenUsedInSql(subObj);
          if (needsQuote) buf.writeByte((byte) '"').writeStringEscapeDoubleQuoteBegin();
          try {
            componentConverter.convertFrom(textFormat, subObj, buf);
          } finally {
            if (needsQuote) buf.writeStringEscapeDoubleQuoteEnd();
          }
          if (needsQuote) buf.writeByte((byte) '"');
        }
      }
      buf.writeByte((byte) '}');
    }
  }
}
