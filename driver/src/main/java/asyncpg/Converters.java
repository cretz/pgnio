package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;

import static asyncpg.DataType.*;

public interface Converters {
  static List<Converters> loadConverters() {
    List<Converters> ret = new ArrayList<>();
    ServiceLoader.load(Converters.class).iterator().forEachRemaining(ret::add);
    ret.sort(Comparator.comparing(Converters::getPriority));
    return ret;
  }

  static Map<String, Converters.To> loadAllToConverters() {
    Map<String, Converters.To> ret = new HashMap<>();
    for (Converters convs : loadConverters()) ret.putAll(convs.loadToConverters());
    return ret;
  }

  static Map<String, Converters.From> loadAllFromConverters() {
    Map<String, Converters.From> ret = new HashMap<>();
    for (Converters convs : loadConverters()) ret.putAll(convs.loadFromConverters());
    return ret;
  }

  default double getPriority() { return 1; }
  Map<String, Converters.To> loadToConverters();
  Map<String, Converters.From> loadFromConverters();

  @FunctionalInterface
  interface To<T> {
    default @Nullable T convertToNullable(
        QueryMessage.RowMeta.Column column, byte@Nullable [] bytes) throws Exception {
      return convertToNullable(column.dataTypeOid, column.formatText, bytes);
    }

    default @Nullable T convertToNullable(
        int dataTypeOid, boolean formatText, byte@Nullable [] bytes) throws Exception {
      return bytes == null ? null : convertTo(dataTypeOid, formatText, bytes);
    }

    // If this returns null, it is assumed this cannot decode it
    @Nullable T convertTo(int dataTypeOid, boolean formatText, byte[] bytes) throws Exception;
  }

  @FunctionalInterface
  interface From<T> {
    void convertFrom(boolean textFormat, T obj, BufWriter buf) throws Exception;
  }

  class BuiltIn implements Converters {
    @Override
    public Map<String, To> loadToConverters() { return TO_CONVERTERS; }

    @Override
    public Map<String, From> loadFromConverters() { return FROM_CONVERTERS; }

    public static final Map<String, Converters.To> TO_CONVERTERS;
    public static final Map<String, Converters.From> FROM_CONVERTERS = Collections.emptyMap();

    protected static final DateTimeFormatter TIMESTAMP_FORMAT = new DateTimeFormatterBuilder().
        parseCaseInsensitive().append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').
        append(DateTimeFormatter.ISO_LOCAL_TIME).toFormatter();
    protected static final DateTimeFormatter TIMESTAMPTZ_FORMAT = new DateTimeFormatterBuilder().
        parseCaseInsensitive().append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').
        append(DateTimeFormatter.ISO_LOCAL_TIME).appendOffset("+HH:mm", "").toFormatter();
    protected static final DateTimeFormatter TIMETZ_FORMAT = new DateTimeFormatterBuilder().
        parseCaseInsensitive().append(DateTimeFormatter.ISO_LOCAL_TIME).
        appendOffset("+HH:mm", "").toFormatter();

    static {
      Map<String, Converters.To> def = new HashMap<>();
      def.put(byte[].class.getName(), BuiltIn::convertToByteArray);
      def.put(BigDecimal.class.getName(), BuiltIn::convertToBigDecimal);
      def.put(BigInteger.class.getName(), BuiltIn::convertToBigInteger);
      def.put(ByteBuffer.class.getName(), BuiltIn::convertToByteBuffer);
      def.put(Character.class.getName(), BuiltIn::convertToCharacter);
      def.put(Double.class.getName(), BuiltIn::convertToDouble);
      def.put(Float.class.getName(), BuiltIn::convertToFloat);
      def.put(Integer.class.getName(), BuiltIn::convertToInteger);
      def.put(LocalDate.class.getName(), BuiltIn::convertToLocalDate);
      def.put(LocalDateTime.class.getName(), BuiltIn::convertToLocalDateTime);
      def.put(LocalTime.class.getName(), BuiltIn::convertToLocalTime);
      def.put(Long.class.getName(), BuiltIn::convertToLong);
      def.put(DataType.Money.class.getName(), BuiltIn::convertToMoney);
      def.put(OffsetDateTime.class.getName(), BuiltIn::convertToOffsetDateTime);
      def.put(OffsetTime.class.getName(), BuiltIn::convertToOffsetTime);
      def.put(Short.class.getName(), BuiltIn::convertToShort);
      def.put(String.class.getName(), BuiltIn::convertToString);
      TO_CONVERTERS = Collections.unmodifiableMap(def);
    }

    protected static void assertNotBinary(boolean formatText) {
      if (!formatText) throw new UnsupportedOperationException("Binary not supported yet");
    }

    public static @Nullable BigDecimal convertToBigDecimal(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INT2:
        case INT4:
        case INT8:
        case NUMERIC:
        case FLOAT4:
        case FLOAT8:
          return new BigDecimal(convertToString(bytes));
        case MONEY:
          Money money = convertToMoney(dataTypeOid, formatText, bytes);
          return money == null ? null : money.value;
        default:
          return null;
      }
    }

    public static @Nullable BigInteger convertToBigInteger(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INT2:
        case INT4:
        case INT8:
          // We try on numeric and will let the parser fail if there is a decimal
        case NUMERIC:
          return new BigInteger(convertToString(bytes));
        default:
          return null;
      }
    }

    public static byte@Nullable[] convertToByteArray(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case BYTEA:
          String str = convertToString(bytes);
          if (!str.startsWith("\\x")) throw new IllegalArgumentException("Expected bytea type");
          return Util.hexToBytes(str.substring(2));
        default:
          return null;
      }
    }

    public static @Nullable ByteBuffer convertToByteBuffer(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      byte[] retBytes = convertToByteArray(dataTypeOid, formatText, bytes);
      return retBytes == null ? null : ByteBuffer.wrap(retBytes);
    }

    public static @Nullable Character convertToCharacter(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      // If string isn't a single char, return null which reports error
      String str = convertToString(dataTypeOid, formatText, bytes);
      return str == null || str.length() != 1 ? null : str.charAt(0);
    }

    public static @Nullable Double convertToDouble(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INT2:
        case INT4:
        case INT8:
        case NUMERIC:
        case FLOAT4:
        case FLOAT8:
          return Double.valueOf(convertToString(bytes));
        default:
          return null;
      }
    }

    public static @Nullable Float convertToFloat(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INT2:
        case INT4:
        case INT8:
        case NUMERIC:
        case FLOAT4:
          return Float.valueOf(convertToString(bytes));
        default:
          return null;
      }
    }

    public static @Nullable Integer convertToInteger(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INT2:
        case INT4:
          return Integer.valueOf(convertToString(bytes));
        default:
          return null;
      }
    }

    public static @Nullable LocalDate convertToLocalDate(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case DATE:
          return LocalDate.parse(convertToString(bytes), DateTimeFormatter.ISO_LOCAL_DATE);
        default:
          return null;
      }
    }

    public static @Nullable LocalDateTime convertToLocalDateTime(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case TIMESTAMP:
          return LocalDateTime.parse(convertToString(bytes), TIMESTAMP_FORMAT);
        default:
          return null;
      }
    }

    public static @Nullable LocalTime convertToLocalTime(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case TIME:
          return LocalTime.parse(convertToString(bytes), DateTimeFormatter.ISO_LOCAL_TIME);
        default:
          return null;
      }
    }

    public static @Nullable Long convertToLong(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INT2:
        case INT4:
        case INT8:
          return Long.valueOf(convertToString(bytes));
        default:
          return null;
      }
    }

    public static @Nullable Money convertToMoney(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case MONEY:
          return Money.fromString(convertToString(bytes));
        default:
          return null;
      }
    }

    public static @Nullable OffsetDateTime convertToOffsetDateTime(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case TIMESTAMPTZ:
          return OffsetDateTime.parse(convertToString(bytes), TIMESTAMPTZ_FORMAT);
        default:
          return null;
      }
    }

    public static @Nullable OffsetTime convertToOffsetTime(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case TIMETZ:
          return OffsetTime.parse(convertToString(bytes), TIMETZ_FORMAT);
        default:
          return null;
      }
    }

    public static @Nullable Short convertToShort(int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INT2:
          return Short.valueOf(convertToString(bytes));
        default:
          return null;
      }
    }

    public static String convertToString(byte[] bytes) throws Exception {
      return Util.threadLocalStringDecoder.get().decode(ByteBuffer.wrap(bytes)).toString();
    }

    public static @Nullable String convertToString(int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case TEXT:
        case VARCHAR:
        case BPCHAR:
        case NAME:
        case CHAR:
        case UUID:
        case JSON:
          return convertToString(bytes);
        default:
          return null;
      }
    }
  }
}
