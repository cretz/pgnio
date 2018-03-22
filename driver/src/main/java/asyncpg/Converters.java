package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.function.Function;

import static asyncpg.DataType.*;

/**
 * Interface for sets of converters. These are loaded via {@link ServiceLoader} for instances of this interface.
 */
public interface Converters {

  /** Use {@link ServiceLoader} to load converter sets and return in order of priority (lower first) */
  static List<Converters> loadConverters() {
    List<Converters> ret = new ArrayList<>();
    ServiceLoader.load(Converters.class).iterator().forEachRemaining(ret::add);
    ret.sort(Comparator.comparing(Converters::getPriority));
    return ret;
  }

  /** Shortcut for {@link #loadConverters()} + {@link #loadToConverters()} */
  static Map<String, Converters.To> loadAllToConverters() {
    Map<String, Converters.To> ret = new HashMap<>();
    for (Converters convs : loadConverters()) ret.putAll(convs.loadToConverters());
    return ret;
  }

  /** Shortcut for {@link #loadConverters()} + {@link #loadFromConverters()} */
  static Map<String, Converters.From> loadAllFromConverters() {
    Map<String, Converters.From> ret = new HashMap<>();
    for (Converters convs : loadConverters()) ret.putAll(convs.loadFromConverters());
    return ret;
  }

  /**
   * The priority of this converter set vs others. Lower priority number is used first. Default value is 1, built-in
   * converters are 0.
   */
  default double getPriority() { return 1; }
  /** Load to-converters in this set keyed by class name */
  Map<String, Converters.To> loadToConverters();
  /** Load from-converters in this set keyed by class name */
  Map<String, Converters.From> loadFromConverters();

  /**
   * A "to converter" which is a converter that converts from a Postgres value to a Java value. While there are a couple
   * of methods here, implementers only need to implement {@link #convertTo(int, boolean, byte[])}. For most uses, only
   * the text format needs to be supported.
   */
  @FunctionalInterface
  interface To<T> {
    /**
     * What the delimiter is when reading arrays of this type. In Postgres this is always ',' (the default) except for
     * the geometric "box" type.
     */
    default char arrayDelimiter() { return ','; }

    /** Shortcut for {@link #convertToNullable(QueryMessage.RowMeta.Column, byte[])} */
    default @Nullable T convertToNullable(
        QueryMessage.RowMeta.Column column, byte@Nullable [] bytes) {
      return convertToNullable(column.dataTypeOid, column.textFormat, bytes);
    }

    /**
     * Returns null if bytes are null, otherwise defers to {@link #convertTo(int, boolean, byte[])}. This method can be
     * overridden if there is a need to read certain non-null values as null.
     */
    default @Nullable T convertToNullable(
        int dataTypeOid, boolean textFormat, byte@Nullable [] bytes) {
      return bytes == null ? null : convertTo(dataTypeOid, textFormat, bytes);
    }

    /**
     * Convert from the given non-null byte array to the type. A null response here doesn't mean the value is null.
     * Rather, null should be returned when the conversion can't be performed for the given data type OID. Data type
     * OIDs are enumerated in {@link DataType}. For most uses, textFormat will always be true and
     * {@link BuiltIn#assertNotBinary(boolean)} can be called if necessary.
     */
    @Nullable T convertTo(int dataTypeOid, boolean textFormat, byte[] bytes);
  }

  /**
   * A "from converter" which is a converter that converts from a Java value to a Postgres value. While there are a
   * couple of methods here, implementers only need to implement {@link #convertFrom(boolean, Object, BufWriter)}. For
   * most uses, only the text format needs to be supported.
   */
  @FunctionalInterface
  interface From<T> {
    /**
     * Whether, when serializing as part of SQL, this needs to be quoted. The default implementation returns true for
     * everything except numeric values with all numeric characters.
     */
    default boolean mustBeQuotedWhenUsedInSql(T obj) {
      if (!(obj instanceof Number)) return true;
      if (obj instanceof Double) {
        Double dbl = (Double) obj;
        return dbl.isNaN() || dbl.isInfinite() || dbl.compareTo(-0.0d) == 0;
      }
      if (obj instanceof Float) {
        Float flt = (Float) obj;
        return flt.isNaN() || flt.isInfinite() || flt.compareTo(-0.0f) == 0;
      }
      return false;
    }

    /**
     * What the delimiter is when writing arrays of this type. In Postgres this is always ',' (the default) except for
     * the geometric "box" type.
     */
    default char arrayDelimiter() { return ','; }

    /**
     * For the given obj and format, write the serialized form to buf. For most uses, textFormat will always be true and
     * {@link BuiltIn#assertNotBinary(boolean)} can be called if necessary.
     */
    void convertFrom(boolean textFormat, T obj, BufWriter buf);
  }

  /** Collection of built-in conversions for all common types */
  class BuiltIn implements Converters {
    @Override
    public double getPriority() { return 0; }

    @Override
    public Map<String, To> loadToConverters() { return TO_CONVERTERS; }

    @Override
    public Map<String, From> loadFromConverters() { return FROM_CONVERTERS; }

    /** Read-only collection of to-converters keyed by class name */
    public static final Map<String, Converters.To> TO_CONVERTERS;
    /** Read-only collection of from-converters keyed by class name */
    public static final Map<String, Converters.From> FROM_CONVERTERS;

    protected static final DateTimeFormatter TIMESTAMP_FORMAT = new DateTimeFormatterBuilder().
        parseCaseInsensitive().append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').
        append(DateTimeFormatter.ISO_LOCAL_TIME).toFormatter();
    protected static final DateTimeFormatter TIMESTAMPTZ_FORMAT = new DateTimeFormatterBuilder().
        parseCaseInsensitive().append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').
        append(DateTimeFormatter.ISO_LOCAL_TIME).appendOffset("+HH:mm", "").toFormatter();
    protected static final DateTimeFormatter TIMETZ_FORMAT = new DateTimeFormatterBuilder().
        parseCaseInsensitive().append(DateTimeFormatter.ISO_LOCAL_TIME).
        appendOffset("+HH:mm", "").toFormatter();

    @SuppressWarnings("unchecked")
    protected static <T> Converters.From typedFrom(Converters.From<T> from) {
      return (a, b, c) -> from.convertFrom(a, (T) b, c);
    }

    static {
      Map<String, Converters.From> from = new HashMap<>();
      from.put(byte[].class.getName(), convertTextFromItem((byte[] v) -> "\\x" + Util.bytesToHex(v)));
      from.put(BigDecimal.class.getName(), convertTextFromItem(Object::toString));
      from.put(BigInteger.class.getName(), convertTextFromItem(Object::toString));
      from.put(Boolean.class.getName(), convertTextFromItem(Object::toString));
      from.put(Box.class.getName(), new Converters.From<Box>() {
        final Converters.From<Box> delegate = convertTextFromItem(Object::toString);
        @Override
        public char arrayDelimiter() { return ';'; }
        @Override
        public void convertFrom(boolean textFormat, Box obj, BufWriter buf) {
          delegate.convertFrom(textFormat, obj, buf);
        }
      });
      from.put(ByteBuffer.class.getName(), convertTextFromItem((ByteBuffer v) ->
          "\\x" + Util.bytesToHex(Arrays.copyOfRange(v.array(), v.position(), v.limit()))));
      from.put(Character.class.getName(), convertTextFromItem(Object::toString));
      from.put(Circle.class.getName(), convertTextFromItem(Object::toString));
      from.put(Double.class.getName(), convertTextFromItem(Object::toString));
      from.put(Float.class.getName(), convertTextFromItem(Object::toString));
      from.put(Inet.class.getName(), convertTextFromItem(Object::toString));
      from.put(Integer.class.getName(), convertTextFromItem(Object::toString));
      from.put(Interval.class.getName(), convertTextFromItem(Object::toString));
      from.put(Line.class.getName(), convertTextFromItem(Object::toString));
      from.put(LineSegment.class.getName(), convertTextFromItem(Object::toString));
      from.put(LocalDate.class.getName(), convertTextFromItem(DateTimeFormatter.ISO_LOCAL_DATE::format));
      from.put(LocalDateTime.class.getName(), convertTextFromItem(TIMESTAMP_FORMAT::format));
      from.put(LocalTime.class.getName(), convertTextFromItem(DateTimeFormatter.ISO_LOCAL_TIME::format));
      from.put(Long.class.getName(), convertTextFromItem(Object::toString));
      from.put(MacAddr.class.getName(), convertTextFromItem(Object::toString));
      from.put(Money.class.getName(), convertTextFromItem(Object::toString));
      from.put(OffsetDateTime.class.getName(), convertTextFromItem(TIMESTAMPTZ_FORMAT::format));
      from.put(OffsetTime.class.getName(), convertTextFromItem(TIMETZ_FORMAT::format));
      from.put(Path.class.getName(), convertTextFromItem(Object::toString));
      from.put(Point.class.getName(), convertTextFromItem(Object::toString));
      from.put(Polygon.class.getName(), convertTextFromItem(Object::toString));
      from.put(Short.class.getName(), convertTextFromItem(Object::toString));
      from.put(String.class.getName(), convertTextFromItem(Object::toString));
      from.put(java.util.UUID.class.getName(), convertTextFromItem(Object::toString));
      FROM_CONVERTERS = Collections.unmodifiableMap(from);

      Map<String, Converters.To> to = new HashMap<>();
      to.put(byte[].class.getName(), convertTextToItem(v -> {
        if (!v.startsWith("\\x")) throw new IllegalArgumentException("Expected bytea type");
        return Util.hexToBytes(v.substring(2));
      }, DataType.BYTEA));
      to.put(BigDecimal.class.getName(), convertTextToItem(BigDecimal::new,
          DataType.INT2, DataType.INT4, DataType.INT8, DataType.NUMERIC, DataType.FLOAT4, DataType.FLOAT8));
      to.put(BigInteger.class.getName(), convertTextToItem(BigInteger::new,
          DataType.INT2, DataType.INT4, DataType.INT8, DataType.NUMERIC));
      to.put(Boolean.class.getName(), convertTextBytesToItem(v -> v[0] == 't', DataType.BOOL));
      to.put(Box.class.getName(), new Converters.To<Box>() {
        @SuppressWarnings("type.argument.type.incompatible")
        final Converters.To<Box> delegate = convertTextToItem(Box::valueOf, DataType.BOX);
        @Override
        public char arrayDelimiter() { return ';'; }
        @Override
        public @Nullable Box convertTo(int dataTypeOid, boolean textFormat, byte[] bytes) {
          return delegate.convertTo(dataTypeOid, textFormat, bytes);
        }
      });
      to.put(ByteBuffer.class.getName(), convertTextToItem(v -> {
        if (!v.startsWith("\\x")) throw new IllegalArgumentException("Expected bytea type");
        return ByteBuffer.wrap(Util.hexToBytes(v.substring(2)));
      }, DataType.BYTEA));
      to.put(Character.class.getName(), convertTextToItem(v -> v == null || v.length() != 1 ? null : v.charAt(0),
          DataType.TEXT, DataType.VARCHAR, DataType.BPCHAR, DataType.CHAR, DataType.BIT));
      to.put(Circle.class.getName(), convertTextToItem(Circle::valueOf, DataType.CIRCLE));
      to.put(Double.class.getName(), convertTextToItem(Double::valueOf,
          DataType.INT2, DataType.INT4, DataType.INT8, DataType.NUMERIC, DataType.FLOAT4, DataType.FLOAT8));
      to.put(Float.class.getName(), convertTextToItem(Float::valueOf,
          DataType.INT2, DataType.INT4, DataType.INT8, DataType.NUMERIC, DataType.FLOAT4));
      to.put(Inet.class.getName(), convertTextToItem(Inet::valueOf, DataType.CIDR, DataType.INET));
      to.put(Integer.class.getName(), convertTextToItem(Integer::valueOf, DataType.INT2, DataType.INT4));
      to.put(Interval.class.getName(), convertTextToItem(Interval::valueOf, DataType.INTERVAL));
      to.put(Line.class.getName(), convertTextToItem(Line::valueOf, DataType.LINE));
      to.put(LineSegment.class.getName(), convertTextToItem(LineSegment::valueOf, DataType.LSEG));
      to.put(LocalDate.class.getName(), convertTextToItem(v -> LocalDate.parse(v, DateTimeFormatter.ISO_LOCAL_DATE),
          DataType.DATE));
      to.put(LocalDateTime.class.getName(), convertTextToItem(v -> LocalDateTime.parse(v, TIMESTAMP_FORMAT),
          DataType.TIMESTAMP));
      to.put(LocalTime.class.getName(), convertTextToItem(v -> LocalTime.parse(v, DateTimeFormatter.ISO_LOCAL_TIME),
          DataType.TIME));
      to.put(Long.class.getName(), convertTextToItem(Long::valueOf, DataType.INT2, DataType.INT4, DataType.INT8));
      to.put(MacAddr.class.getName(), convertTextToItem(MacAddr::valueOf, DataType.MACADDR, DataType.MACADDR8));
      to.put(Money.class.getName(), convertTextToItem(Money::valueOf, DataType.MONEY));
      to.put(OffsetDateTime.class.getName(), convertTextToItem(v -> OffsetDateTime.parse(v, TIMESTAMPTZ_FORMAT),
          DataType.TIMESTAMPTZ));
      to.put(OffsetTime.class.getName(), convertTextToItem(v -> OffsetTime.parse(v, TIMETZ_FORMAT), DataType.TIMETZ));
      to.put(Path.class.getName(), convertTextToItem(Path::valueOf, DataType.PATH));
      to.put(Point.class.getName(), convertTextToItem(Point::valueOf, DataType.POINT));
      to.put(Polygon.class.getName(), convertTextToItem(Polygon::valueOf, DataType.POLYGON));
      to.put(Short.class.getName(), convertTextToItem(Short::valueOf, DataType.INT2));
      to.put(String.class.getName(), convertTextToItem(Function.identity(),
          DataType.TEXT, DataType.VARCHAR, DataType.BPCHAR, DataType.NAME, DataType.NUMERIC, DataType.CHAR,
          DataType.UUID, DataType.JSON, DataType.JSONB, DataType.BIT, DataType.VARBIT, DataType.CIDR, DataType.INET,
          DataType.XML));
      to.put(java.util.UUID.class.getName(), convertTextToItem(java.util.UUID::fromString, DataType.UUID));
      TO_CONVERTERS = Collections.unmodifiableMap(to);
    }

    protected static void assertNotBinary(boolean textFormat) {
      if (!textFormat) throw new UnsupportedOperationException("Binary not supported yet");
    }

    /** Helper to create a from-converter from an item-to-string text-format-only function */
    public static <T> Converters.From<T> convertTextFromItem(Function<T, String> fn) {
      return (textFormat, obj, buf) -> {
        assertNotBinary(textFormat);
        buf.writeString(fn.apply(obj));
      };
    }

    /**
     * Helper to create a to-converter from a byte-array-to-item text-format-only function. It accepts some data type
     * OIDs (see {@link DataType}) that are allowed ({@link DataType#UNSPECIFIED} is implied even if not set). Note,
     * the vararg array might be mutated to make for fast lookup.
     */
    public static <@Nullable T> Converters.To<T> convertTextBytesToItem(Function<byte[], T> fn, int... dataTypeOids) {
      if (dataTypeOids.length != 1) Arrays.sort(dataTypeOids);
      return (dataTypeOid, textFormat, bytes) -> {
        assertNotBinary(textFormat);
        dataTypeOid = DataType.normalizeOid(dataTypeOid);
        if (dataTypeOid != DataType.UNSPECIFIED &&
            ((dataTypeOids.length == 1 && dataTypeOid != dataTypeOids[0]) ||
            (Arrays.binarySearch(dataTypeOids, dataTypeOid) < 0))) return null;
        return fn.apply(bytes);
      };
    }

    /**
     * Shortcut for {@link #convertTextBytesToItem(Function, int...)} that automatically converts from byte array to
     * string
     */
    public static <@Nullable T> Converters.To<T> convertTextToItem(Function<String, T> fn, int... dataTypeOids) {
      return convertTextBytesToItem(((Function<byte[], String>) Util::stringFromBytes).andThen(fn), dataTypeOids);
    }
  }
}
