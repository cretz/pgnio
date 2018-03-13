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
        QueryMessage.RowMeta.Column column, byte@Nullable [] bytes) {
      return convertToNullable(column.dataTypeOid, column.textFormat, bytes);
    }

    default @Nullable T convertToNullable(
        int dataTypeOid, boolean textFormat, byte@Nullable [] bytes) {
      return bytes == null ? null : convertTo(dataTypeOid, textFormat, bytes);
    }

    // If this returns null, it is assumed this cannot decode it
    @Nullable T convertTo(int dataTypeOid, boolean textFormat, byte[] bytes);
  }

  @FunctionalInterface
  interface From<T> {
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

    void convertFrom(boolean textFormat, T obj, BufWriter buf);
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
      from.put(Double.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(Float.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(Integer.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(Interval.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(Long.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(LocalDate.class.getName(), typedFrom(BuiltIn::convertFromLocalDate));
      from.put(LocalDateTime.class.getName(), typedFrom(BuiltIn::convertFromLocalDateTime));
      from.put(LocalTime.class.getName(), typedFrom(BuiltIn::convertFromLocalTime));
      from.put(DataType.Money.class.getName(), BuiltIn::convertFromAnyToString);
      from.put(OffsetDateTime.class.getName(), typedFrom(BuiltIn::convertFromOffsetDateTime));
      from.put(OffsetTime.class.getName(), typedFrom(BuiltIn::convertFromOffsetTime));
      from.put(Point.class.getName(), BuiltIn::convertFromAnyToString);
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
      to.put(Point.class.getName(), BuiltIn::convertToPoint);
      to.put(Short.class.getName(), BuiltIn::convertToShort);
      to.put(String.class.getName(), BuiltIn::convertToString);
      TO_CONVERTERS = Collections.unmodifiableMap(to);
    }

    protected static void assertNotBinary(boolean textFormat) {
      if (!textFormat) throw new UnsupportedOperationException("Binary not supported yet");
    }

    public static void convertFromAnyToString(boolean textFormat, Object obj, BufWriter buf) {
      assertNotBinary(textFormat);
      buf.writeString(obj.toString());
    }

    public static void convertFromByteArray(boolean textFormat, byte[] obj, BufWriter buf) {
      assertNotBinary(textFormat);
      buf.writeString("\\x" + Util.bytesToHex(obj));
    }

    public static void convertFromByteBuffer(boolean textFormat, ByteBuffer byteBuf, BufWriter buf) {
      // We'll follow other Java libs and read remaining on byte buf; but we'll put the position back
      byte[] bytes = new byte[byteBuf.limit() - byteBuf.position()];
      int prevPos = byteBuf.position();
      byteBuf.get(bytes);
      byteBuf.position(prevPos);
      convertFromByteArray(textFormat, bytes, buf);
    }

    public static void convertFromLocalDate(boolean textFormat, LocalDate obj, BufWriter buf) {
      assertNotBinary(textFormat);
      buf.writeString(obj.format(DateTimeFormatter.ISO_LOCAL_DATE));
    }

    public static void convertFromLocalDateTime(boolean textFormat, LocalDateTime obj, BufWriter buf) {
      assertNotBinary(textFormat);
      buf.writeString(obj.format(TIMESTAMP_FORMAT));
    }

    public static void convertFromLocalTime(boolean textFormat, LocalTime obj, BufWriter buf) {
      assertNotBinary(textFormat);
      buf.writeString(obj.format(DateTimeFormatter.ISO_LOCAL_TIME));
    }

    public static void convertFromOffsetDateTime(boolean textFormat, OffsetDateTime obj, BufWriter buf) {
      assertNotBinary(textFormat);
      buf.writeString(obj.format(TIMESTAMPTZ_FORMAT));
    }

    public static void convertFromOffsetTime(boolean textFormat, OffsetTime obj, BufWriter buf) {
      assertNotBinary(textFormat);
      buf.writeString(obj.format(TIMETZ_FORMAT));
    }

    public static @Nullable BigDecimal convertToBigDecimal(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INT2:
        case INT4:
        case INT8:
        case NUMERIC:
        case FLOAT4:
        case FLOAT8:
          return new BigDecimal(Util.stringFromBytes(bytes));
        case MONEY:
          Money money = convertToMoney(dataTypeOid, textFormat, bytes);
          return money == null ? null : money.value;
        default:
          return null;
      }
    }

    public static @Nullable BigInteger convertToBigInteger(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INT2:
        case INT4:
        case INT8:
          // We try on numeric and will let the parser fail if there is a decimal
        case NUMERIC:
          return new BigInteger(Util.stringFromBytes(bytes));
        default:
          return null;
      }
    }

    public static @Nullable Boolean convertToBoolean(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case BOOL:
          return bytes[0] == 't';
        default:
          return null;
      }
    }

    public static byte@Nullable[] convertToByteArray(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case BYTEA:
          String str = Util.stringFromBytes(bytes);
          if (!str.startsWith("\\x")) throw new IllegalArgumentException("Expected bytea type");
          return Util.hexToBytes(str.substring(2));
        default:
          return null;
      }
    }

    public static @Nullable ByteBuffer convertToByteBuffer(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      byte[] retBytes = convertToByteArray(dataTypeOid, textFormat, bytes);
      return retBytes == null ? null : ByteBuffer.wrap(retBytes);
    }

    public static @Nullable Character convertToCharacter(int dataTypeOid, boolean textFormat, byte[] bytes) {
      // If string isn't a single char, return null which reports error
      String str = convertToString(dataTypeOid, textFormat, bytes);
      return str == null || str.length() != 1 ? null : str.charAt(0);
    }

    public static @Nullable Double convertToDouble(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INT2:
        case INT4:
        case INT8:
        case NUMERIC:
        case FLOAT4:
        case FLOAT8:
          return Double.valueOf(Util.stringFromBytes(bytes));
        default:
          return null;
      }
    }

    public static @Nullable Float convertToFloat(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INT2:
        case INT4:
        case INT8:
        case NUMERIC:
        case FLOAT4:
          return Float.valueOf(Util.stringFromBytes(bytes));
        default:
          return null;
      }
    }

    public static @Nullable Integer convertToInteger(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INT2:
        case INT4:
          return Integer.valueOf(Util.stringFromBytes(bytes));
        default:
          return null;
      }
    }

    public static @Nullable Interval convertToInterval(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INTERVAL:
          // Format: [N year[s]] [N mon[s]] [N day[s]] [ISO 8601 local time]
          // Empty: ISO 8601 local time
          String str = Util.stringFromBytes(bytes);
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

    public static @Nullable LocalDate convertToLocalDate(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case DATE:
          return LocalDate.parse(Util.stringFromBytes(bytes), DateTimeFormatter.ISO_LOCAL_DATE);
        default:
          return null;
      }
    }

    public static @Nullable LocalDateTime convertToLocalDateTime(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case TIMESTAMP:
          return LocalDateTime.parse(Util.stringFromBytes(bytes), TIMESTAMP_FORMAT);
        default:
          return null;
      }
    }

    public static @Nullable LocalTime convertToLocalTime(
        int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case TIME:
          return LocalTime.parse(Util.stringFromBytes(bytes), DateTimeFormatter.ISO_LOCAL_TIME);
        default:
          return null;
      }
    }

    public static @Nullable Long convertToLong(
        int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INT2:
        case INT4:
        case INT8:
          return Long.valueOf(Util.stringFromBytes(bytes));
        default:
          return null;
      }
    }

    public static @Nullable Money convertToMoney(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case MONEY:
          return Money.fromString(Util.stringFromBytes(bytes));
        default:
          return null;
      }
    }

    public static @Nullable OffsetDateTime convertToOffsetDateTime(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case TIMESTAMPTZ:
          return OffsetDateTime.parse(Util.stringFromBytes(bytes), TIMESTAMPTZ_FORMAT);
        default:
          return null;
      }
    }

    public static @Nullable OffsetTime convertToOffsetTime(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case TIMETZ:
          return OffsetTime.parse(Util.stringFromBytes(bytes), TIMETZ_FORMAT);
        default:
          return null;
      }
    }

    public static @Nullable Point convertToPoint(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case POINT:
          // Format: (x,y)
          String str = Util.stringFromBytes(bytes);
          int commaIndex = str.indexOf(',');
          if (str.isEmpty() || str.charAt(0) != '(' || str.charAt(str.length() - 1) != ')' || commaIndex == -1)
            throw new IllegalArgumentException("Unrecognized point format: " + str);
          return new Point(Integer.parseInt(str.substring(1, commaIndex)),
              Integer.parseInt(str.substring(commaIndex + 1, str.length() - 1)));
        default:
          return null;
      }
    }

    public static @Nullable Short convertToShort(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (dataTypeOid) {
        case UNSPECIFIED:
        case INT2:
          return Short.valueOf(Util.stringFromBytes(bytes));
        default:
          return null;
      }
    }

    public static @Nullable String convertToString(int dataTypeOid, boolean textFormat, byte[] bytes) {
      assertNotBinary(textFormat);
      switch (DataType.normalizeOid(dataTypeOid)) {
        case UNSPECIFIED:
        case TEXT:
        case VARCHAR:
        case BPCHAR:
        case NAME:
        case CHAR:
        case UUID:
        case JSON:
          return Util.stringFromBytes(bytes);
        default:
          return null;
      }
    }
  }
}
