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
    void convertFrom(boolean formatText, T obj, BufWriter buf) throws Exception;
  }

  class BuiltIn implements Converters {
    @Override
    public double getPriority() { return 0; }

    @Override
    public Map<String, To> loadToConverters() { return TO_CONVERTERS; }

    @Override
    public Map<String, From> loadFromConverters() { return FROM_CONVERTERS; }

    public static final Map<String, Converters.To> TO_CONVERTERS;
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
      from.put(byte[].class.getName(), typedFrom(BuiltIn::convertFromByteArray));
      from.put(BigDecimal.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(BigInteger.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(Boolean.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(ByteBuffer.class.getName(), typedFrom(BuiltIn::convertFromByteBuffer));
      from.put(Character.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(Double.class.getName(), typedFrom(BuiltIn::convertFromDouble));
      from.put(Float.class.getName(), typedFrom(BuiltIn::convertFromFloat));
      from.put(Integer.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(Interval.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(Long.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(DataType.Money.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(Short.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(String.class.getName(), BuiltIn::convertFromAnyToString);
      FROM_CONVERTERS = Collections.unmodifiableMap(from);

      Map<String, Converters.To> to = new HashMap<>();
      to.put(byte[].class.getName(), BuiltIn::convertToByteArray);
      to.put(BigDecimal.class.getName(), BuiltIn::convertToBigDecimal);
      to.put(BigInteger.class.getName(), BuiltIn::convertToBigInteger);
      to.put(Boolean.class.getName(), BuiltIn::convertToBoolean);
      to.put(ByteBuffer.class.getName(), BuiltIn::convertToByteBuffer);
      to.put(Character.class.getName(), BuiltIn::convertToCharacter);
      to.put(Double.class.getName(), BuiltIn::convertToDouble);
      to.put(Float.class.getName(), BuiltIn::convertToFloat);
      to.put(Integer.class.getName(), BuiltIn::convertToInteger);
      to.put(Interval.class.getName(), BuiltIn::convertToInterval);
      to.put(LocalDate.class.getName(), BuiltIn::convertToLocalDate);
      to.put(LocalDateTime.class.getName(), BuiltIn::convertToLocalDateTime);
      to.put(LocalTime.class.getName(), BuiltIn::convertToLocalTime);
      to.put(Long.class.getName(), BuiltIn::convertToLong);
      to.put(DataType.Money.class.getName(), BuiltIn::convertToMoney);
      to.put(OffsetDateTime.class.getName(), BuiltIn::convertToOffsetDateTime);
      to.put(OffsetTime.class.getName(), BuiltIn::convertToOffsetTime);
      to.put(Short.class.getName(), BuiltIn::convertToShort);
      to.put(String.class.getName(), BuiltIn::convertToString);
      TO_CONVERTERS = Collections.unmodifiableMap(to);
    }

    protected static void assertNotBinary(boolean formatText) {
      if (!formatText) throw new UnsupportedOperationException("Binary not supported yet");
    }

    public static void convertFromAnyToString(boolean formatText, Object obj, BufWriter buf) {
      assertNotBinary(formatText);
      buf.writeString(obj.toString());
    }

    public static void convertFromByteArray(boolean formatText, byte[] obj, BufWriter buf) {
      assertNotBinary(formatText);
      buf.writeString("\\x" + Util.bytesToHex(obj));
    }

    public static void convertFromByteBuffer(boolean formatText, ByteBuffer byteBuf, BufWriter buf) {
      // We'll follow other Java libs and read remaining on byte buf; but we'll put the position back
      byte[] bytes = new byte[byteBuf.limit() - byteBuf.position()];
      int prevPos = byteBuf.position();
      byteBuf.get(bytes);
      byteBuf.position(prevPos);
      convertFromByteArray(formatText, bytes, buf);
    }

    public static void convertFromDouble(boolean formatText, Double obj, BufWriter buf) {
      assertNotBinary(formatText);
      if (obj.isInfinite() || obj.isNaN() || obj.equals(-0.0d)) buf.writeString("'" + obj + "'");
      else buf.writeString(obj.toString());
    }

    public static void convertFromFloat(boolean formatText, Float obj, BufWriter buf) {
      assertNotBinary(formatText);
      if (obj.isInfinite() || obj.isNaN() || obj.equals(-0.0f)) buf.writeString("'" + obj + "'");
      else buf.writeString(obj.toString());
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

    public static @Nullable Boolean convertToBoolean(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case BOOL:
          return bytes[0] == 't';
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

    public static @Nullable Interval convertToInterval(
        int dataTypeOid, boolean formatText, byte[] bytes) throws Exception {
      assertNotBinary(formatText);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INTERVAL:
          // Format: [N year[s]] [N mon[s]] [N day[s]] [ISO 8601 local time]
          // Empty: ISO 8601 local time
          String str = convertToString(bytes);
          List<String> pieces = Util.splitByChar(str, ' ');
          boolean hasTime = pieces.size() % 2 == 1;
          int datePieceMax = hasTime ? pieces.size() - 1 : pieces.size();
          int years = 0, mons = 0, days = 0;
          for (int i = 0; i < datePieceMax; i += 2) {
            int val = Integer.parseInt(pieces.get(i));
            switch (pieces.get(i + 1)) {
              case "year":
              case "years":
                years = val;
                break;
              case "mon":
              case "mons":
                mons = val;
                break;
              case "day":
              case "days":
                days = val;
                break;
              default:
                throw new IllegalArgumentException("Unrecognized piece '" + pieces.get(i + 1) + "' in '" + str + "'");
            }
          }
          Duration timeDuration = Duration.ZERO;
          if (hasTime) {
            String timePiece = pieces.get(pieces.size() - 1);
            boolean negative = timePiece.charAt(0) == '-';
            if (negative) timePiece = timePiece.substring(1);
            LocalTime parsed = LocalTime.parse(timePiece, DateTimeFormatter.ISO_LOCAL_TIME);
            timeDuration = Duration.ofSeconds(negative ? -parsed.toSecondOfDay() : parsed.toSecondOfDay(),
                negative ? -parsed.getNano() : parsed.getNano());
          }
          return new Interval(Period.of(years, mons, days), timeDuration);
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
