package asyncpg;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RowReaderTest extends DbTestBase {

  @Test
  public void testTypesNormalValues() {
    assertColChecks("row_reader_test_normal",
        colCheck("smallint", (short) 123),
        colCheck("integer", 123));
  }

  void assertColChecks(String tableName, ColCheck... colChecks) {
    QueryMessage.Row row = withConnectionSync(conn ->
        createTable(conn, tableName, colChecks).thenCompose(__ ->
            insertThenSelect(conn, tableName, colChecks)));
    Assert.assertNotNull(row);
    for (ColCheck colCheck : colChecks) colCheck.assertInRow(row);
  }

  CompletableFuture<?> createTable(QueryReadyConnection.AutoCommit conn, String name, ColCheck... colChecks) {
    String cols = Arrays.stream(colChecks).map(c -> c.colName + " " + c.dbType).collect(Collectors.joining(","));
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

  @SuppressWarnings("unchecked")
  <T> void assertColEquals(QueryMessage.Row row, String colName, T expected) {
    T actual = RowReader.DEFAULT.get(row, colName, (Class<T>) expected.getClass());
    Assert.assertEquals("Failed matching column: " + colName, expected, actual);
  }

  static <T> ColCheck<T> colCheck(String dbType, T expectedVal) {
    return colCheck(dbType, expectedVal.toString(), expectedVal);
  }

  @SuppressWarnings("unchecked")
  static <T> ColCheck<T> colCheck(String dbType, String valAsString, T expectedVal) {
    return colCheck(dbType, valAsString, expectedVal, (Class<T>) expectedVal.getClass());
  }

  static <T> ColCheck<T> colCheck(String dbType, String valAsString, T expectedVal, Class<T> valClass) {
    return colCheck("val_" + dbType, dbType, valAsString, expectedVal, valClass);
  }

  static <T> ColCheck<T> colCheck(String colName, String dbType, String valAsString, T expectedVal, Class<T> valClass) {
    return new ColCheck<>(colName, dbType, valAsString, expectedVal, valClass);
  }

  static class ColCheck<T> {
    final String colName;
    final String dbType;
    final String valAsString;
    final T expectedVal;
    final Class<T> valClass;

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
  }
}
