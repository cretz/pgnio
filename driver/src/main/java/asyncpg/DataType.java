package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.Duration;
import java.time.Period;
import java.util.*;

public class DataType {
  public static final int UNSPECIFIED = 0;

  public static final int BIT = 1560;
  public static final int BIT_ARRAY = 1561;
  public static final int BOOL = 16;
  public static final int BOOL_ARRAY = 1000;
  public static final int BOX = 603;
  public static final int BOX_ARRAY = 1020;
  public static final int BPCHAR = 1042;
  public static final int BPCHAR_ARRAY = 1014;
  public static final int BYTEA = 17;
  public static final int BYTEA_ARRAY = 1001;
  public static final int CHAR = 18;
  public static final int CHAR_ARRAY = 1002;
  public static final int CIRCLE = 718;
  public static final int CIRCLE_ARRAY = 719;
  public static final int DATE = 1082;
  public static final int DATE_ARRAY = 1182;
  public static final int FLOAT4 = 700;
  public static final int FLOAT4_ARRAY = 1021;
  public static final int FLOAT8 = 701;
  public static final int FLOAT8_ARRAY = 1022;
  public static final int INT2 = 21;
  public static final int INT2_ARRAY = 1005;
  public static final int INT4 = 23;
  public static final int INT4_ARRAY = 1007;
  public static final int INT8 = 20;
  public static final int INT8_ARRAY = 1016;
  public static final int INTERVAL = 1186;
  public static final int INTERVAL_ARRAY = 1187;
  public static final int JSON = 114;
  public static final int JSON_ARRAY = 199;
  public static final int JSONB = 3802;
  public static final int JSONB_ARRAY = 3807;
  public static final int LINE = 628;
  public static final int LINE_ARRAY = 629;
  public static final int LSEG = 601;
  public static final int LSEG_ARRAY = 1018;
  public static final int MONEY = 790;
  public static final int MONEY_ARRAY = 791;
  public static final int NAME = 19;
  public static final int NAME_ARRAY = 1003;
  public static final int NUMERIC = 1700;
  public static final int NUMERIC_ARRAY = 1231;
  public static final int OID = 26;
  public static final int OID_ARRAY = 1028;
  public static final int PATH = 602;
  public static final int PATH_ARRAY = 1019;
  public static final int POINT = 600;
  public static final int POINT_ARRAY = 1017;
  public static final int POLYGON = 604;
  public static final int POLYGON_ARRAY = 1027;
  public static final int REF_CURSOR = 1790;
  public static final int REF_CURSOR_ARRAY = 2201;
  public static final int TEXT = 25;
  public static final int TEXT_ARRAY = 1009;
  public static final int TIME = 1083;
  public static final int TIME_ARRAY = 1183;
  public static final int TIMESTAMP = 1114;
  public static final int TIMESTAMP_ARRAY = 1115;
  public static final int TIMESTAMPTZ = 1184;
  public static final int TIMESTAMPTZ_ARRAY = 1185;
  public static final int TIMETZ = 1266;
  public static final int TIMETZ_ARRAY = 1270;
  public static final int UUID = 2950;
  public static final int UUID_ARRAY = 2951;
  public static final int VARBIT = 1562;
  public static final int VARBIT_ARRAY = 1563;
  public static final int VARCHAR = 1043;
  public static final int VARCHAR_ARRAY = 1015;
  public static final int VOID = 2278;
  public static final int XML = 142;
  public static final int XML_ARRAY = 143;

  private static final Map<Integer, String> dataTypes;

  static {
    Map<Integer, String> map = new HashMap<>();
    map.put(UNSPECIFIED, "UNSPECIFIED");
    map.put(BIT, "BIT");
    map.put(BIT_ARRAY, "BIT_ARRAY");
    map.put(BOOL, "BOOL");
    map.put(BOOL_ARRAY, "BOOL_ARRAY");
    map.put(BOX, "BOX");
    map.put(BOX_ARRAY, "BOX_ARRAY");
    map.put(BPCHAR, "BPCHAR");
    map.put(BPCHAR_ARRAY, "BPCHAR_ARRAY");
    map.put(BYTEA, "BYTEA");
    map.put(BYTEA_ARRAY, "BYTEA_ARRAY");
    map.put(CHAR, "CHAR");
    map.put(CHAR_ARRAY, "CHAR_ARRAY");
    map.put(CIRCLE, "CIRCLE");
    map.put(CIRCLE_ARRAY, "CIRCLE_ARRAY");
    map.put(DATE, "DATE");
    map.put(DATE_ARRAY, "DATE_ARRAY");
    map.put(FLOAT4, "FLOAT4");
    map.put(FLOAT4_ARRAY, "FLOAT4_ARRAY");
    map.put(FLOAT8, "FLOAT8");
    map.put(FLOAT8_ARRAY, "FLOAT8_ARRAY");
    map.put(INT2, "INT2");
    map.put(INT2_ARRAY, "INT2_ARRAY");
    map.put(INT4, "INT4");
    map.put(INT4_ARRAY, "INT4_ARRAY");
    map.put(INT8, "INT8");
    map.put(INT8_ARRAY, "INT8_ARRAY");
    map.put(INTERVAL, "INTERVAL");
    map.put(INTERVAL_ARRAY, "INTERVAL_ARRAY");
    map.put(JSON, "JSON");
    map.put(JSON_ARRAY, "JSON_ARRAY");
    map.put(JSONB, "JSONB");
    map.put(JSONB_ARRAY, "JSONB_ARRAY");
    map.put(LINE, "LINE");
    map.put(LINE_ARRAY, "LINE_ARRAY");
    map.put(LSEG, "LSEG");
    map.put(LSEG_ARRAY, "LSEG_ARRAY");
    map.put(MONEY, "MONEY");
    map.put(MONEY_ARRAY, "MONEY_ARRAY");
    map.put(NAME, "NAME");
    map.put(NAME_ARRAY, "NAME_ARRAY");
    map.put(NUMERIC, "NUMERIC");
    map.put(NUMERIC_ARRAY, "NUMERIC_ARRAY");
    map.put(OID, "OID");
    map.put(OID_ARRAY, "OID_ARRAY");
    map.put(PATH, "PATH");
    map.put(PATH_ARRAY, "PATH_ARRAY");
    map.put(POINT, "POINT");
    map.put(POINT_ARRAY, "POINT_ARRAY");
    map.put(POLYGON, "POLYGON");
    map.put(POLYGON_ARRAY, "POLYGON_ARRAY");
    map.put(REF_CURSOR, "REF_CURSOR");
    map.put(REF_CURSOR_ARRAY, "REF_CURSOR_ARRAY");
    map.put(TEXT, "TEXT");
    map.put(TEXT_ARRAY, "TEXT_ARRAY");
    map.put(TIME, "TIME");
    map.put(TIME_ARRAY, "TIME_ARRAY");
    map.put(TIMESTAMP, "TIMESTAMP");
    map.put(TIMESTAMP_ARRAY, "TIMESTAMP_ARRAY");
    map.put(TIMESTAMPTZ, "TIMESTAMPTZ");
    map.put(TIMESTAMPTZ_ARRAY, "TIMESTAMPTZ_ARRAY");
    map.put(TIMETZ, "TIMETZ");
    map.put(TIMETZ_ARRAY, "TIMETZ_ARRAY");
    map.put(UUID, "UUID");
    map.put(UUID_ARRAY, "UUID_ARRAY");
    map.put(VARBIT, "VARBIT");
    map.put(VARBIT_ARRAY, "VARBIT_ARRAY");
    map.put(VARCHAR, "VARCHAR");
    map.put(VARCHAR_ARRAY, "VARCHAR_ARRAY");
    map.put(VOID, "VOID");
    map.put(XML, "XML");
    map.put(XML_ARRAY, "XML_ARRAY");
    dataTypes = new HashMap<>(map.size());
    dataTypes.putAll(map);
  }

  public static @Nullable String nameForOid(int oid) { return dataTypes.get(oid); }

  // Returns unspecified if unknown
  public static int normalizeOid(int oid) { return dataTypes.containsKey(oid) ? oid : UNSPECIFIED; }

  // Either the non-array form or unspecified
  public static int arrayComponentOid(int oid) {
    switch (oid) {
      case BIT_ARRAY: return BIT;
      case BOOL_ARRAY: return BOOL;
      case BOX_ARRAY: return BOX;
      case BPCHAR_ARRAY: return BPCHAR;
      case BYTEA_ARRAY: return BYTEA;
      case CHAR_ARRAY: return CHAR;
      case CIRCLE_ARRAY: return CIRCLE;
      case DATE_ARRAY: return DATE;
      case FLOAT4_ARRAY: return FLOAT4;
      case FLOAT8_ARRAY: return FLOAT8;
      case INT2_ARRAY: return INT2;
      case INT4_ARRAY: return INT4;
      case INT8_ARRAY: return INT8;
      case INTERVAL_ARRAY: return INTERVAL;
      case JSON_ARRAY: return JSON;
      case JSONB_ARRAY: return JSONB;
      case LINE_ARRAY: return LINE;
      case LSEG_ARRAY: return LSEG;
      case MONEY_ARRAY: return MONEY;
      case NAME_ARRAY: return NAME;
      case NUMERIC_ARRAY: return NUMERIC;
      case OID_ARRAY: return OID;
      case PATH_ARRAY: return PATH;
      case POINT_ARRAY: return POINT;
      case POLYGON_ARRAY: return POLYGON;
      case REF_CURSOR_ARRAY: return REF_CURSOR;
      case TEXT_ARRAY: return TEXT;
      case TIME_ARRAY: return TIME;
      case TIMESTAMP_ARRAY: return TIMESTAMP;
      case TIMESTAMPTZ_ARRAY: return TIMESTAMPTZ;
      case TIMETZ_ARRAY: return TIMETZ;
      case UUID_ARRAY: return UUID;
      case VARBIT_ARRAY: return VARBIT;
      case VARCHAR_ARRAY: return VARCHAR;
      case XML_ARRAY: return XML;
      default: return UNSPECIFIED;
    }
  }

  private DataType() { }

  public static class Money {
    public static Money fromString(String string) { return fromString(string, null); }
    public static Money fromString(String string, @Nullable Locale locale) {
      // Format is like $100 or ($300.00)
      boolean negative = string.charAt(0) == '(' && string.charAt(string.length() - 1) == ')';
      if (negative) string = string.substring(1, string.length() - 1);
      int digitIndex = 0;
      while (digitIndex < string.length() && !Character.isDigit(string.charAt(digitIndex))) digitIndex++;
      if (digitIndex == 0 || digitIndex == string.length()) throw new NumberFormatException("Missing symbol or digits");
      String symbol = string.substring(0, digitIndex);
      String value = string.substring(digitIndex);
      DecimalFormat fmt = (DecimalFormat) (locale == null ?
          NumberFormat.getInstance() : NumberFormat.getInstance(locale));
      fmt.setParseBigDecimal(true);
      try {
        return new Money(symbol, (BigDecimal) fmt.parse(value));
      } catch (ParseException e) { throw new RuntimeException(e); }
    }

    public final String symbol;
    public final BigDecimal value;

    public Money(String symbol, BigDecimal value) {
      this.symbol = symbol;
      this.value = value;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Money money = (Money) o;
      return Objects.equals(symbol, money.symbol) && Objects.equals(value, money.value);
    }

    @Override
    public int hashCode() { return Objects.hash(symbol, value); }

    @Override
    public String toString() { return symbol + value; }
  }

  public static class Interval {
    public final Period datePeriod;
    public final Duration timeDuration;

    public Interval(Period datePeriod, Duration timeDuration) {
      this.datePeriod = datePeriod;
      if (Math.abs(timeDuration.getSeconds()) > 24 * 3600)
        throw new IllegalArgumentException("Given duration is more than 24 hours: '" + timeDuration + "'");
      this.timeDuration = timeDuration;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Interval interval = (Interval) o;
      return Objects.equals(datePeriod, interval.datePeriod) && Objects.equals(timeDuration, interval.timeDuration);
    }

    @Override
    public int hashCode() { return Objects.hash(datePeriod, timeDuration); }

    @Override
    public String toString() {
      StringBuilder ret = new StringBuilder();
      if (datePeriod.getYears() != 0) ret.append(datePeriod.getYears()).append(" years ");
      if (datePeriod.getMonths() != 0) ret.append(datePeriod.getMonths()).append(" mons ");
      if (datePeriod.getDays() != 0) ret.append(datePeriod.getDays()).append(" days ");
      if (ret.length() == 0 || !timeDuration.isZero()) {
        if (timeDuration.isNegative()) ret.append('-');
        long seconds = Math.abs(timeDuration.getSeconds());
        long hours = seconds / 3600;
        if (hours < 10) ret.append('0');
        ret.append(hours).append(':');
        seconds %= 3600;
        long minutes = seconds / 60;
        if (minutes < 10) ret.append('0');
        ret.append(minutes).append(':');
        seconds %= 60;
        // Algorithm from Duration::toString
        if (timeDuration.isNegative() && timeDuration.getNano() > 0) seconds--;
        ret.append(seconds);
        if (timeDuration.getNano() > 0) {
          int pos = ret.length();
          if (timeDuration.isNegative()) ret.append(2 * 1000_000_000L - timeDuration.getNano());
          else ret.append(timeDuration.getNano() + 1000_000_000L);
          while (ret.charAt(ret.length() - 1) == '0') ret.setLength(ret.length() - 1);
          ret.setCharAt(pos, '.');
        }
      }
      return ret.toString().trim();
    }
  }

  public static class Point {
    public final int x;
    public final int y;

    public Point(int x, int y) {
      this.x = x;
      this.y = y;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Point point = (Point) o;
      return x == point.x && y == point.y;
    }

    @Override
    public int hashCode() { return Objects.hash(x, y); }

    @Override
    public String toString() { return "(" + x + "," + y + ")"; }
  }

  public static class Line {
    public final int a;
    public final int b;
    public final int c;

    public Line(int a, int b, int c) {
      this.a = a;
      this.b = b;
      this.c = c;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Line line = (Line) o;
      return a == line.a && b == line.b && c == line.c;
    }

    @Override
    public int hashCode() { return Objects.hash(a, b, c); }

    @Override
    public String toString() { return "{" + a + "," + b + "," + c + "}"; }
  }

  public static class LineSegment {
    public final Point point1;
    public final Point point2;

    public LineSegment(Point point1, Point point2) {
      this.point1 = point1;
      this.point2 = point2;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LineSegment that = (LineSegment) o;
      return Objects.equals(point1, that.point1) && Objects.equals(point2, that.point2);
    }

    @Override
    public int hashCode() { return Objects.hash(point1, point2); }

    @Override
    public String toString() { return "(" + point1 + "," + point2 + ")"; }
  }

  public static class Box {
    public final Point point1;
    public final Point point2;

    public Box(Point point1, Point point2) {
      this.point1 = point1;
      this.point2 = point2;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Box that = (Box) o;
      return Objects.equals(point1, that.point1) && Objects.equals(point2, that.point2);
    }

    @Override
    public int hashCode() { return Objects.hash(point1, point2); }

    @Override
    public String toString() { return "(" + point1 + "," + point2 + ")"; }
  }

  public static class Path {
    public final Point[] points;
    public final boolean closed;

    public Path(Point[] points, boolean closed) {
      this.points = points;
      this.closed = closed;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Path path = (Path) o;
      return closed == path.closed && Arrays.equals(points, path.points);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(closed);
      result = 31 * result + Arrays.hashCode(points);
      return result;
    }

    @Override
    public String toString() {
      StringJoiner joiner = new StringJoiner(",");
      for (Point point : points) joiner.add(point.toString());
      if (closed) return "(" + joiner.toString() + ")";
      return "[" + joiner.toString() + "]";
    }
  }

  public static class Polygon {
    public final Path path;

    public Polygon(Path path) {
      if (!path.closed) throw new IllegalArgumentException("Polygon paths are closed");
      this.path = path;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Polygon polygon = (Polygon) o;
      return Objects.equals(path, polygon.path);
    }

    @Override
    public int hashCode() { return Objects.hash(path); }

    @Override
    public String toString() { return path.toString(); }
  }

  public static class Circle {
    public final Point center;
    public final int radius;

    public Circle(Point center, int radius) {
      this.center = center;
      this.radius = radius;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Circle circle = (Circle) o;
      return radius == circle.radius && Objects.equals(center, circle.center);
    }

    @Override
    public int hashCode() { return Objects.hash(center, radius); }

    @Override
    public String toString() { return "<" + center + "," + radius + ">"; }
  }
}
