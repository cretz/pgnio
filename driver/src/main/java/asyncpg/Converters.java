package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
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

    static {
      Map<String, Converters.To> def = new HashMap<>();
      def.put(BigDecimal.class.getName(), BuiltIn::convertToBigDecimal);
      def.put(BigInteger.class.getName(), BuiltIn::convertToBigInteger);
      def.put(Double.class.getName(), BuiltIn::convertToDouble);
      def.put(Float.class.getName(), BuiltIn::convertToFloat);
      def.put(Integer.class.getName(), BuiltIn::convertToInteger);
      def.put(Long.class.getName(), BuiltIn::convertToLong);
      def.put(DataType.Money.class.getName(), BuiltIn::convertToMoney);
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
        case VARCHAR:
          return convertToString(bytes);
        default:
          return null;
      }
    }
  }
}
