package asyncpg;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;

@RunWith(Parameterized.class)
@SuppressWarnings("unchecked")
public class QueryTest extends DbTestBase {

  @Parameterized.Parameters(name = "{0}")
  public static List<TypeCheck> data() {
    return Arrays.asList(
        TypeCheck.of("smallint", (short) 100, Short.MIN_VALUE, Short.MAX_VALUE, null),
        TypeCheck.of("integer", 100, Integer.MIN_VALUE, Integer.MAX_VALUE, null)
        // TODO: migrate everything else over from RowReaderTest
    );
  }

  @Parameterized.Parameter
  public TypeCheck typeCheck;

  @Test
  public void testSimpleQuery() {
    String tableName = "test_simple_query_" + typeCheck.safeName;
    // Create the table, insert, select, drop table
    QueryMessage.Row row = withConnectionSync(conn ->
        createTable(conn, tableName).
            thenCompose(__ -> simpleInsertThenSelect(conn, tableName)).
            thenCompose(r -> dropTable(conn, tableName).thenApply(__ -> r)));
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

  CompletableFuture<?> createTable(QueryReadyConnection.AutoCommit conn, String name) {
    String[] cols = new String[typeCheck.interestingVals.length];
    for (int i = 0; i < typeCheck.interestingVals.length; i++) {
      boolean nullable = typeCheck.interestingVals[i] == null;
      cols[i] = "col_" + typeCheck.safeName + "_" + i + " " +
          typeCheck.dbType + (nullable ? " NULL" : " NOT NULL");
    }
    return conn.simpleQueryExec("CREATE TABLE " + name + "(" + String.join(",", cols) + ")");
  }

  CompletableFuture<QueryMessage.Row> simpleInsertThenSelect(
      QueryReadyConnection.AutoCommit conn, String tableName) {
    String[] vals = new String[typeCheck.interestingVals.length];
    for (int i = 0; i < vals.length; i++) {
      try {
        vals[i] = typeCheck.valToSql(typeCheck.interestingVals[i]);
      } catch (Exception e) { throw new RuntimeException(e); }
    }
    return conn.simpleQueryExec("INSERT INTO " + tableName + " VALUES (" + String.join(",", vals) + ")").
        thenCompose(__ -> conn.simpleQueryRows("SELECT * FROM " + tableName)).
        thenApply(rows -> rows.isEmpty() ? null : rows.get(0));
  }

  CompletableFuture<?> dropTable(QueryReadyConnection.AutoCommit conn, String name) {
    return conn.simpleQueryExec("DROP TABLE " + name);
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
      ParamWriter.DEFAULT.write(true, val, valBuf);
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
    BiPredicate<T, T> overrideEquals;

    @SafeVarargs
    TypeCheck(String name, String dbType, Class<T> valClass, T... interestingVals) {
      this.name = name;
      char[] safeNameChars = name.toCharArray();
      for (int i = 0; i < safeNameChars.length; i++)
        if (!Character.isJavaIdentifierPart(safeNameChars[i])) safeNameChars[i] = '_';
      safeName = new String(safeNameChars);
      this.dbType = dbType;
      this.valClass = valClass;
      this.interestingVals = interestingVals;
    }

    String valToSql(T val) throws Exception {
      if (val == null) return "NULL";
      // TODO: support overriding string format
      ByteBuffer buf = ByteBuffer.wrap(valToBytes(val));
      // Trim the trailing 0 if there...
      if (buf.hasRemaining() && buf.get(buf.limit() - 1) == 0) buf.limit(buf.limit() - 1);
      String str = Util.threadLocalStringDecoder.get().decode(buf).toString();
      if (val instanceof Number) return str;
      return "'" + str.replace("'", "''") + "'";
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
  }
}
