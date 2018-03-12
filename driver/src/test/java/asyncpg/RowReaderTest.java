package asyncpg;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

public class RowReaderTest extends DbTestBase {

  /*
  TODO:
  - test big integer w/ decimal
  - test errors
  - test types not the exact type expected
  - test negatives, mins, maxes, other oddities (e.g. +/- nan, +/- inf, etc)
    - includes infinity on date types too
  - test alternative locales
  - internal types such as: "char", name
  */

  @Test
  public void testSimpleNumeric() {
    assertColChecks("row_reader_test_simple_numeric",
        colCheck("smallint", (short) 100),
        colCheck("integer", 101),
        colCheck("bigint", 102L),
        colCheck("numeric(9, 3)", new BigDecimal("1.030")),
        colCheck("real", 1.04f),
        colCheck("double precision", 1.05d),
        colCheck("smallserial", (short) 106).notNull(),
        colCheck("serial", 107).notNull(),
        colCheck("bigserial", 108L).notNull());
  }

  @Test
  public void testSimpleMonetary() {
    assertColChecks("row_reader_test_simple_monetary",
        colCheck("money", new DataType.Money("$", new BigDecimal("1.01"))),
        colCheck("money", "'$1.02'", new BigDecimal("1.02")).colName("val_money2"));
  }

  @Test
  public void testSimpleCharacter() {
    assertColChecks("row_reader_test_simple_character",
        colCheck("varchar(10)", "test1"),
        colCheck("char(10)", "'test2'", "test2     "),
        colCheck("char(1)", 'Q'));
  }

  @Test
  public void testSimpleBinary() {
    assertColChecks("row_reader_test_simple_binary",
        colCheck("bytea", "'\\x" + Util.bytesToHex("test1".getBytes()) + "'",
            "test1".getBytes()).overrideEquals(Arrays::equals),
        colCheck("bytea", "'\\x" + Util.bytesToHex("test2".getBytes()) + "'",
            ByteBuffer.wrap("test2".getBytes()), ByteBuffer.class).colName("val_bytea2"));
  }

  @Test
  public void testSimpleTemporal() {
    assertColChecks("row_reader_test_simple_temporal",
        colCheck("date", "'2018-01-01'", LocalDate.of(2018, 1, 1)),
        colCheck("time", "'00:01:01'", LocalTime.of(0, 1, 1)),
        colCheck("time(3)", "'00:01:02.345'", LocalTime.of(0, 1, 2, 345000000)),
        colCheck("time with time zone", "'00:01:03 EST'", OffsetTime.of(0, 1, 3, 0, ZoneOffset.ofHours(-5))),
        colCheck("time(1) with time zone", "'00:01:04.5 CST'",
            OffsetTime.of(0, 1, 4, 500000000, ZoneOffset.ofHours(-6))),
        colCheck("timestamp", "'2018-01-02 00:02:01'",
            LocalDateTime.of(2018, 1, 2, 0, 2, 1)),
        colCheck("timestamp(3)", "'2018-01-03 00:02:02.123'",
            LocalDateTime.of(2018, 1, 3, 0, 2, 2, 123000000)),
        colCheck("timestamp with time zone", "'2018-01-04 00:02:03-5:30'",
            OffsetDateTime.of(2018, 1, 4, 0, 2, 3, 0, ZoneOffset.ofHoursMinutes(-5, -30)).
                atZoneSameInstant(ZoneId.systemDefault()).toOffsetDateTime()),
        colCheck("timestamp(2) with time zone", "'2018-01-05 00:02:04.23-6:30'",
            OffsetDateTime.of(2018, 1, 5, 0, 2, 4, 230000000, ZoneOffset.ofHoursMinutes(-6, -30)).
                atZoneSameInstant(ZoneId.systemDefault()).toOffsetDateTime()),
        colCheck("interval", new DataType.Interval(Period.of(1, 2, 3), Duration.ofSeconds(4000, 500000))),
        colCheck("interval", new DataType.Interval(Period.of(-1, -2, -3), Duration.ofSeconds(-4000, -500000))).
            colName("val_interval2"));
  }

  @Test
  public void testNull() {
    assertColChecks("row_reader_test_null",
        colCheckNull("smallint", Short.class),
        colCheckNull("integer", Integer.class),
        colCheckNull("bigint", Long.class),
        colCheckNull("numeric(9, 3)", BigDecimal.class),
        colCheckNull("real", Float.class),
        colCheckNull("double precision", Double.class),
        colCheckNull("money", DataType.Money.class),
        colCheckNull("varchar(10)", String.class),
        colCheckNull("char(10)", String.class),
        colCheckNull("char(1)", Character.class),
        colCheckNull("bytea", byte[].class),
        colCheckNull("bytea", ByteBuffer.class).colName("val_bytea2"),
        colCheckNull("date", LocalDate.class),
        colCheckNull("time", LocalTime.class),
        colCheckNull("time with time zone", OffsetTime.class),
        colCheckNull("timestamp", LocalDateTime.class),
        colCheckNull("timestamp with time zone", OffsetDateTime.class));
  }

  void assertColChecks(String tableName, ColCheck... colChecks) {
    QueryMessage.Row row = withConnectionSync(conn ->
        createTable(conn, tableName, colChecks).thenCompose(__ ->
            insertThenSelect(conn, tableName, colChecks)));
    Assert.assertNotNull(row);
    for (ColCheck colCheck : colChecks) colCheck.assertInRow(row);
  }

  CompletableFuture<?> createTable(QueryReadyConnection.AutoCommit conn, String name, ColCheck... colChecks) {
    String cols = Arrays.stream(colChecks).map(c ->
        c.colName + " " + c.dbType + (c.nullable ? " NULL" : " NOT NULL")).collect(Collectors.joining(","));
    return conn.simpleQueryExec("CREATE TABLE " + name + "(" + cols + ")");
  }

  CompletableFuture<QueryMessage.Row> insertThenSelect(
      QueryReadyConnection.AutoCommit conn, String tableName, ColCheck... colChecks) {
    String insert = "INSERT INTO " + tableName + " (" +
        Arrays.stream(colChecks).map(c -> c.colName).collect(Collectors.joining(",")) + ") VALUES (" +
        Arrays.stream(colChecks).map(c -> c.valAsString).collect(Collectors.joining(",")) + ")";
    return conn.simpleQueryExec(insert).thenCompose(__ ->
        conn.simpleQueryRows("SELECT * FROM " + tableName).thenApply(rows ->
            rows.isEmpty() ? null : rows.get(0)));
  }

  static <T> ColCheck<T> colCheck(String dbType, T expectedVal) {
    return colCheck(dbType, expectedVal instanceof Number ?
        expectedVal.toString() : "'" + expectedVal.toString().replace("'", "''") + "'", expectedVal);
  }

  @SuppressWarnings("unchecked")
  static <T> ColCheck<T> colCheck(String dbType, String valAsString, T expectedVal) {
    return colCheck(dbType, valAsString, expectedVal, (Class<T>) expectedVal.getClass());
  }

  static <T> ColCheck<T> colCheckNull(String dbType, Class<T> valClass) {
    return colCheck(dbType, "null", null, valClass);
  }

  static <T> ColCheck<T> colCheck(String dbType, String valAsString, T expectedVal, Class<T> valClass) {
    char[] safeDbType = dbType.toCharArray();
    for (int i = 0; i < safeDbType.length; i++)
      if (!Character.isJavaIdentifierPart(safeDbType[i])) safeDbType[i] = '_';
    return colCheck("val_" + String.valueOf(safeDbType), dbType, valAsString, expectedVal, valClass);
  }

  static <T> ColCheck<T> colCheck(String colName, String dbType, String valAsString, T expectedVal, Class<T> valClass) {
    return new ColCheck<>(colName, dbType, valAsString, expectedVal, valClass);
  }

  static class ColCheck<T> {
    String colName;
    String dbType;
    String valAsString;
    T expectedVal;
    Class<T> valClass;
    boolean nullable = true;
    BiPredicate<T, T> overrideEquals;

    ColCheck(String colName, String dbType, String valAsString, T expectedVal, Class<T> valClass) {
      this.colName = colName;
      this.dbType = dbType;
      this.valAsString = valAsString;
      this.expectedVal = expectedVal;
      this.valClass = valClass;
    }

    void assertInRow(QueryMessage.Row row) {
      String msg = "Failed col " + colName + " has expected val " + expectedVal + " for type " + valClass;
      T actualVal = RowReader.DEFAULT.get(row, colName, valClass);
      if (overrideEquals != null) {
        if (!overrideEquals.test(expectedVal, actualVal)) Assert.fail(msg);
      } else {
        Assert.assertEquals(msg, expectedVal, actualVal);
      }
    }

    ColCheck<T> colName(String colName) { this.colName = colName; return this; }
    ColCheck<T> valClass(Class<T> valClass) { this.valClass = valClass; return this; }
    ColCheck<T> notNull() { nullable = false; return this; }
    ColCheck<T> overrideEquals(BiPredicate<T, T> overrideEquals) { this.overrideEquals = overrideEquals; return this; }
  }
}
