package asyncpg;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RowReaderTest extends DbTestBase {

  /*
  TODO:
  - test big integer w/ decimal
  - test errors
  - test types not the exact type expected
  - test negatives, mins, maxes, other oddities (e.g. +/- nan, +/- inf, etc)
  - test alternative locales
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
  public void testNull() {
    assertColChecks("row_reader_test_null",
        colCheckNull("smallint", Short.class),
        colCheckNull("integer", Integer.class),
        colCheckNull("bigint", Long.class),
        colCheckNull("numeric(9, 3)", BigDecimal.class),
        colCheckNull("real", Float.class),
        colCheckNull("double precision", Double.class));
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

    ColCheck(String colName, String dbType, String valAsString, T expectedVal, Class<T> valClass) {
      this.colName = colName;
      this.dbType = dbType;
      this.valAsString = valAsString;
      this.expectedVal = expectedVal;
      this.valClass = valClass;
    }

    void assertInRow(QueryMessage.Row row) {
      Assert.assertEquals("Failed col " + colName + " has expected val " + expectedVal + " for type " + valClass,
          expectedVal, RowReader.DEFAULT.get(row, colName, valClass));
    }

    ColCheck<T> colName(String colName) { this.colName = colName; return this; }
    ColCheck<T> notNull() { nullable = false; return this; }
  }
}
