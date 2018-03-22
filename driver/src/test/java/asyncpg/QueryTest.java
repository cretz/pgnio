package asyncpg;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static asyncpg.DataType.*;

@RunWith(Parameterized.class)
@SuppressWarnings("unchecked")
public class QueryTest extends DbTestBase {
  /* TODO:
      - test big integer w/ decimal
      - test errors
      - test types not the exact type expected
      - test null chars
      - test alternative locales
      - internal types such as: "char", name
      - other types:
        - composite types
        - ranges
        - oids
  */

  @Parameterized.Parameters(name = "{0}")
  public static List<TypeCheck> data() throws Exception {
    List<TypeCheck> typeChecks = new ArrayList<>();
    // Regular types
    typeChecks.addAll(Arrays.asList(
        TypeCheck.of("smallint", (short) 100, Short.MIN_VALUE, Short.MAX_VALUE, null),
        TypeCheck.of("integer", 101, Integer.MIN_VALUE, Integer.MAX_VALUE, null),
        TypeCheck.of("integer[]", new int[] { 101, 201 }, new int[] { 102, 202 }).overrideEquals(Arrays::equals),
        TypeCheck.of("bigint", 102L, Long.MIN_VALUE, Long.MAX_VALUE, null),
        TypeCheck.of("numeric(9, 3)", new BigDecimal("1.030"), null),
        TypeCheck.of("numeric", new BigInteger("104000000000000000000"), null),
        TypeCheck.of("real", 1.05f, Float.MIN_VALUE,
            3.40282e+38f, // Float.MAX_VALUE has too many digits
            1.17549e-38f, // Float.MIN_NORMAL has too many digits
            0.0f, -0.0f, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, null),
        TypeCheck.of("double precision", 106d, Double.MIN_VALUE,
            1.7976931348623e+308d, // Double.MAX_VALUE has too many digits
            2.2250738585072e-308d, // Double.MIN_NORMAL has too many digits
            0.0d, -0.0d, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN, null),
        TypeCheck.of("smallserial", (short) 107, Short.MIN_VALUE, Short.MAX_VALUE).arrayNotSupported(),
        TypeCheck.of("serial", 108, Integer.MIN_VALUE, Integer.MAX_VALUE).arrayNotSupported(),
        TypeCheck.of("bigserial", 109L, Long.MIN_VALUE, Long.MAX_VALUE).arrayNotSupported(),
        TypeCheck.of("money", new Money("$", new BigDecimal("1.10")), null),
        TypeCheck.of("varchar(10)", "test1", "t\"es\"t'2", null),
        TypeCheck.of("char(10)", "test3", "t\"es\"t'4", null).overrideEquals((exp, act) ->
            (exp == null && act == null) || (exp != null && act.length() == 10 && exp.equals(act.trim()))),
        TypeCheck.of("char(1)", 'Q', null),
        TypeCheck.of("bytea", "test5".getBytes(), null).overrideEquals(Arrays::equals),
        TypeCheck.of("bytea", ByteBuffer.wrap("test6".getBytes()), null),
        TypeCheck.of("date", LocalDate.of(2018, 1, 1), null),
        TypeCheck.of("time", LocalTime.of(0, 1, 1), null),
        TypeCheck.of("time(3)", LocalTime.of(0, 1, 2, 345000000), null),
        TypeCheck.of("time with time zone", OffsetTime.of(0, 1, 3, 0, ZoneOffset.ofHours(-5)), null),
        TypeCheck.of("time(1) with time zone", OffsetTime.of(0, 1, 4, 500000000, ZoneOffset.ofHours(-6)), null),
        TypeCheck.of("timestamp", LocalDateTime.of(2018, 1, 2, 0, 2, 1), null),
        TypeCheck.of("timestamp(3)", LocalDateTime.of(2018, 1, 3, 0, 2, 2, 123000000), null),
        TypeCheck.of("timestamp with time zone", OffsetDateTime.of(2018, 1, 4, 0, 2, 3, 0,
            ZoneOffset.ofHoursMinutes(-5, -30)).atZoneSameInstant(ZoneId.systemDefault()).toOffsetDateTime(), null),
        TypeCheck.of("timestamp(2) with time zone", OffsetDateTime.of(2018, 1, 5, 0, 2, 4, 230000000,
            ZoneOffset.ofHoursMinutes(-6, -30)).atZoneSameInstant(ZoneId.systemDefault()).toOffsetDateTime(), null),
        TypeCheck.of("interval", new Interval(Period.of(1, 2, 3), Duration.ofSeconds(4000, 500000)),
            new Interval(Period.of(-1, -2, -3), Duration.ofSeconds(-4000, -500000)), null),
        TypeCheck.of("boolean", true, false, null),
        TypeCheck.of("mood", "sad", "ok", "happy", null).
            beforeUse(conn -> conn.simpleQueryExec("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")).
            afterUse(conn -> conn.simpleQueryExec("DROP TYPE mood")),
        TypeCheck.of("point", new Point(30, 40), null),
        TypeCheck.of("line", new Line(1, 2, 3), null),
        TypeCheck.of("lseg", new LineSegment(new Point(1, 2), new Point(3, 4)), null),
        TypeCheck.of("box", new Box(new Point(5, 6), new Point(7, 8)), null),
        TypeCheck.of("path", new Path(new Point[] { new Point(9, 10), new Point(10, 11) }, true),
            new Path(new Point[] { new Point(12, 13), new Point(14, 15) }, false), null),
        TypeCheck.of("polygon", new Polygon(new Point(16, 17), new Point(18, 19)), null),
        TypeCheck.of("circle", new Circle(new Point(20, 21), 22), null),
        TypeCheck.of("cidr", new Inet(InetAddress.getByName("192.168.100.128"), 25),
            new Inet(InetAddress.getByName("2001:4f8:3:ba::"), 64), new Inet(InetAddress.getByName("192.168.100.128")),
            new Inet(InetAddress.getByName("192.168.100.128"), 32), new Inet(InetAddress.getByName("2001:4f8:3:ba::")),
            new Inet(InetAddress.getByName("2001:4f8:3:ba::"), 128), null),
        TypeCheck.of("inet", new Inet(InetAddress.getByName("192.168.100.128"), 25),
            new Inet(InetAddress.getByName("2001:4f8:3:ba::"), 64), new Inet(InetAddress.getByName("192.168.100.128")),
            new Inet(InetAddress.getByName("192.168.100.128"), 32), new Inet(InetAddress.getByName("2001:4f8:3:ba::")),
            new Inet(InetAddress.getByName("2001:4f8:3:ba::"), 128), null),
        TypeCheck.of("macaddr", MacAddr.valueOf("08:00:2b:01:02:03"), null),
        TypeCheck.of("macaddr8", MacAddr.valueOf("08:00:2b:01:02:03:04:05"), null),
        TypeCheck.of("bit", '1', '0', null),
        TypeCheck.of("bit(3)", "101", "010", null),
        TypeCheck.of("bit varying(5)", "101", "010", null),
        TypeCheck.of("uuid",
            java.util.UUID.fromString("00000000-0000-0000-0000-000000000000"),
            java.util.UUID.fromString("f47ac10b-58cc-1372-8567-0e02b2c3d479"),
            java.util.UUID.fromString("f47ac10b-58cc-2372-8567-0e02b2c3d479"),
            java.util.UUID.fromString("f47ac10b-58cc-3372-8567-0e02b2c3d479"),
            java.util.UUID.fromString("f47ac10b-58cc-4372-8567-0e02b2c3d479"), null),
        // TODO: alternate XML encodings?
        TypeCheck.of("xml", "<foo>bar</foo>", null),
        TypeCheck.of("json", "[\"foo\", {\"bar\": \"baz\"}, 17, null]", null),
        TypeCheck.of("jsonb", "[\"foo\", {\"bar\": \"baz\"}, 17, null]", null),
        TypeCheck.of("hstore", testMap1(), null)
    ));
    // Array versions
    List<TypeCheck> arrayTypeChecks = new ArrayList<>();
    for (TypeCheck typeCheck : typeChecks) {
      if (typeCheck.arrayNotSupported) continue;
      // Create array type check w/ another copy of the vals
      TypeCheck arrayTypeCheck = typeCheck.asArray(typeCheck.interestingVals);
      arrayTypeChecks.add(arrayTypeCheck);
      // Also, create a multi-dimensional of length two w/ two copies of this if supported
      if (!typeCheck.multiDimensionalArrayNotSupported) {
        Object doubleArr = Array.newInstance(arrayTypeCheck.valClass, 2);
        Array.set(doubleArr, 0, typeCheck.interestingVals);
        Array.set(doubleArr, 1, typeCheck.interestingVals);
        arrayTypeChecks.add(arrayTypeCheck.asArray((Object[]) doubleArr));
      }
    }
    typeChecks.addAll(arrayTypeChecks);
    return typeChecks;
  }

  protected static Map<String, String> testMap1() {
    Map<String, String> ret = new HashMap<>();
    ret.put("foo", "bar");
    ret.put("baz", null);
    return ret;
  }

  @Parameterized.Parameter
  public TypeCheck typeCheck;

  @Test
  public void testSimpleQuery() {
    String tableName = "test_simple_query_" + typeCheck.safeName;
    QueryMessage.Row row = withConnectionSync(conn ->
        withTable(conn, tableName, c -> simpleInsertThenSelect(c, tableName)));
    assertQueryRow(row);
  }

  @Test
  public void testPreparedQuery() {
    String tableName = "test_prepared_query_" + typeCheck.safeName;
    QueryMessage.Row row = withConnectionSync(conn ->
        withTable(conn, tableName, c -> preparedInsertThenSelect(c, tableName)));
    assertQueryRow(row);
  }

  <T> CompletableFuture<T> withTable(QueryReadyConnection.AutoCommit conn, String tableName,
      Function<QueryReadyConnection.AutoCommit, CompletableFuture<T>> fn) {
    return beforeUse(conn).
        thenCompose(c -> createTable(c, tableName)).
        thenCompose(fn).
        thenCompose(r -> dropTable(conn, tableName).thenCompose(this::afterUse).thenApply(__ -> r));
  }

  CompletableFuture<QueryReadyConnection.AutoCommit> createTable(QueryReadyConnection.AutoCommit conn, String name) {
    String[] cols = new String[typeCheck.interestingVals.length];
    for (int i = 0; i < typeCheck.interestingVals.length; i++) {
      boolean nullable = typeCheck.interestingVals[i] == null;
      cols[i] = "col_" + typeCheck.safeName + "_" + i + " " +
          typeCheck.dbType + (nullable ? " NULL" : " NOT NULL");
    }
    return conn.simpleQueryExec("CREATE TEMP TABLE " + name + " (" + String.join(",", cols) + ")");
  }

  CompletableFuture<QueryReadyConnection.AutoCommit> dropTable(QueryReadyConnection.AutoCommit conn, String name) {
    return conn.simpleQueryExec("DROP TABLE " + name);
  }

  CompletableFuture<QueryReadyConnection.AutoCommit> beforeUse(QueryReadyConnection.AutoCommit conn) {
    if (typeCheck.beforeUse == null) return CompletableFuture.completedFuture(conn);
    Function<QueryReadyConnection.AutoCommit, CompletableFuture<?>> fn = typeCheck.beforeUse;
    return fn.apply(conn).thenApply(__ -> conn);
  }

  CompletableFuture<QueryReadyConnection.AutoCommit> afterUse(QueryReadyConnection.AutoCommit conn) {
    if (typeCheck.afterUse == null) return CompletableFuture.completedFuture(conn);
    Function<QueryReadyConnection.AutoCommit, CompletableFuture<?>> fn = typeCheck.afterUse;
    return fn.apply(conn).thenApply(__ -> conn);
  }

  CompletableFuture<QueryMessage.Row> simpleInsertThenSelect(
      QueryReadyConnection.AutoCommit conn, String tableName) {
    String[] vals = new String[typeCheck.interestingVals.length];
    for (int i = 0; i < vals.length; i++) {
      try {
        vals[i] = typeCheck.valToSql(typeCheck.interestingVals[i]);
      } catch (Exception e) { throw new RuntimeException(e); }
    }
    return conn.simpleQueryRowCount("INSERT INTO " + tableName + " VALUES (" + String.join(",", vals) + ")").
        thenCompose(rowCount -> {
          Assert.assertEquals(1L, rowCount.longValue());
          return conn.simpleQueryRows("SELECT * FROM " + tableName);
        }).
        thenApply(rows -> rows.isEmpty() ? null : rows.get(0));
  }

  CompletableFuture<QueryMessage.Row> preparedInsertThenSelect(
      QueryReadyConnection.AutoCommit conn, String tableName) {
    String[] vals = new String[typeCheck.interestingVals.length];
    Object[] params = new Object[vals.length];
    for (int i = 0; i < vals.length; i++) {
      try {
        vals[i] = "$" + (i + 1);
        params[i] = typeCheck.interestingVals[i];
      } catch (Exception e) { throw new RuntimeException(e); }
    }
    return conn.preparedQueryRowCount("INSERT INTO " + tableName + " VALUES (" + String.join(",", vals) + ")", params).
        thenCompose(rowCount -> {
          Assert.assertEquals(1L, rowCount.longValue());
          return conn.simpleQueryRows("SELECT * FROM " + tableName);
        }).
        thenApply(rows -> rows.isEmpty() ? null : rows.get(0));
  }

  void assertQueryRow(QueryMessage.Row row) {
    // Go over every expected val
    for (int i = 0; i < typeCheck.interestingVals.length; i++) {
      Object expectedVal = typeCheck.interestingVals[i];
      // First by index then by name
      typeCheck.assertValEquals("Failed col " + i + " has expected val " + expectedVal + " for " + typeCheck,
          expectedVal, RowReader.DEFAULT.get(row, i, typeCheck.valClass));
      String colName = "col_" + typeCheck.safeName + "_" + i;
      typeCheck.assertValEquals("Failed col " + colName + " has expected val " + expectedVal + " for " + typeCheck,
          expectedVal, RowReader.DEFAULT.get(row, colName, typeCheck.valClass));
    }
  }

  static class TypeCheck<T> {
    @SuppressWarnings("unchecked")
    static <T> TypeCheck<T> of(String dbType, T... interestingVals) {
      Class<T> valClass = null;
      for (T interestingVal : interestingVals) {
        if (interestingVal != null) {
          valClass = (Class<T>) interestingVal.getClass();
          break;
        }
      }
      Objects.requireNonNull(valClass);
      return new TypeCheck<>(dbType, dbType, valClass, interestingVals);
    }

    static final BufWriter.Simple valBuf = new BufWriter.Simple(true, 1000);
    static synchronized byte[] valToBytes(Object val) {
      valBuf.buf.clear();
      ParamWriter.DEFAULT.write(true, val, valBuf, true);
      valBuf.buf.flip();
      byte[] ret = new byte[valBuf.buf.limit()];
      valBuf.buf.get(ret);
      return ret;
    }

    final String name;
    final String safeName;
    final String dbType;
    final Class<T> valClass;
    final T[] interestingVals;
    Function<QueryReadyConnection.AutoCommit, CompletableFuture<?>> beforeUse;
    Function<QueryReadyConnection.AutoCommit, CompletableFuture<?>> afterUse;
    BiPredicate<T, T> overrideEquals;
    boolean arrayNotSupported;
    boolean multiDimensionalArrayNotSupported;

    TypeCheck(String name, String dbType, Class<T> valClass, T[] interestingVals) {
      this.name = name;
      char[] safeNameChars = name.toCharArray();
      for (int i = 0; i < safeNameChars.length; i++)
        if (!Character.isJavaIdentifierPart(safeNameChars[i])) safeNameChars[i] = '_';
      safeName = new String(safeNameChars);
      this.dbType = dbType;
      this.valClass = valClass;
      this.interestingVals = interestingVals;
    }

    TypeCheck<T> beforeUse(Function<QueryReadyConnection.AutoCommit, CompletableFuture<?>> beforeUse) {
      this.beforeUse = beforeUse;
      return this;
    }

    TypeCheck<T> afterUse(Function<QueryReadyConnection.AutoCommit, CompletableFuture<?>> afterUse) {
      this.afterUse = afterUse;
      return this;
    }

    TypeCheck<T> overrideEquals(BiPredicate<T, T> overrideEquals) { this.overrideEquals = overrideEquals; return this; }
    TypeCheck<T> arrayNotSupported() { this.arrayNotSupported = true; return this; }
    TypeCheck<T> multiDimensionalArrayNotSupported() { this.multiDimensionalArrayNotSupported = true; return this; }

    String valToSql(T val) {
      if (val == null) return "NULL";
      return Util.stringFromBytes(valToBytes(val));
    }

    @Override
    public String toString() { return "typeCheck[" + name + "->" + valClass + "]"; }

    void assertValEquals(String msg, T expectedVal, T actualVal) {
      if (overrideEquals != null) {
        if (!overrideEquals.test(expectedVal, actualVal)) Assert.fail(msg);
      } else {
        Assert.assertEquals(msg, expectedVal, actualVal);
      }
    }

    TypeCheck<T[]> asArray(T[] anotherArray) {
      T[][] arr = (T[][]) Array.newInstance(interestingVals.getClass(), anotherArray == null ? 1 : 2);
      arr[0] = interestingVals;
      if (anotherArray != null) arr[1] = anotherArray;
      TypeCheck<T[]> ret = new TypeCheck(name + "[]", dbType + "[]", interestingVals.getClass(), arr) {
        @Override
        void assertValEquals(String msg, Object expectedVal, Object actualVal) {
          if (overrideEquals != null) super.assertValEquals(msg, expectedVal, actualVal);
          else Assert.assertArrayEquals(msg, (T[]) expectedVal, (T[]) actualVal);
        }
      };
      if (overrideEquals != null) ret.overrideEquals = (arr1, arr2) -> {
        if (arr1 == null) return arr2 == null;
        if (arr2 == null || arr1.length != arr2.length) return false;
        for (int i = 0; i < arr1.length; i++) if (!overrideEquals.test(arr1[i], arr2[i])) return false;
        return true;
      };
      ret.beforeUse = beforeUse;
      ret.afterUse = afterUse;
      return ret;
    }
  }
}
