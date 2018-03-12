package asyncpg;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.function.Function;

@RunWith(Parameterized.class)
@SuppressWarnings("unchecked")
public class QueryTest extends DbTestBase {

  /* TODO:
      - test big integer w/ decimal
      - test errors
      - test types not the exact type expected
      - test other oddities (e.g. +/- nan, +/- inf, etc)
        - null chars
      - test alternative locales
      - internal types such as: "char", name
      - other types:
        - enums
        - geom types
  */

  @Parameterized.Parameters(name = "{0}")
  public static List<TypeCheck> data() {
    return Arrays.asList(
        TypeCheck.of("smallint", (short) 100, Short.MIN_VALUE, Short.MAX_VALUE, null),
        TypeCheck.of("integer", 101, Integer.MIN_VALUE, Integer.MAX_VALUE, null),
        TypeCheck.of("bigint", 102L, Long.MIN_VALUE, Long.MAX_VALUE, null),
        TypeCheck.of("numeric(9, 3)", new BigDecimal("1.030"), null),
        TypeCheck.of("numeric", new BigInteger("104000000000000000000"), null),
        TypeCheck.of("real", 1.05f, Float.MIN_VALUE,
            /* Float.MAX_VALUE has too many digits */ 3.40282e+38f,
            /* Float.MIN_NORMAL has too many digits */ 1.17549e-38f,
            0.0f, -0.0f, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, null),
        TypeCheck.of("double precision", 106d, Double.MIN_VALUE,
            /* Double.MAX_VALUE has too many digits */ 1.7976931348623e+308d,
            /* Double.MIN_NORMAL has too many digits */ 2.2250738585072e-308d,
            0.0d, -0.0d, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN, null),
        TypeCheck.of("smallserial", (short) 107, Short.MIN_VALUE, Short.MAX_VALUE),
        TypeCheck.of("serial", 108, Integer.MIN_VALUE, Integer.MAX_VALUE),
        TypeCheck.of("bigserial", 109L, Long.MIN_VALUE, Long.MAX_VALUE),
        TypeCheck.of("money", new DataType.Money("$", new BigDecimal("1.10")), null),
        TypeCheck.of("money", new BigDecimal("1.11"), null).overrideToSql(v -> "'$" + v.toString() + "'"),
        TypeCheck.of("varchar(10)", "test1", "t\"es\"t2", null),
        TypeCheck.of("char(10)", "test3", "t\"es\"t4", null).overrideEquals((exp, act) ->
          (exp == null && act == null) || (exp != null && act.length() == 10 && exp.equals(act.trim()))),
        TypeCheck.of("char(1)", 'Q', null),
        TypeCheck.of("bytea", "test5".getBytes(), null).overrideEquals(Arrays::equals),
        TypeCheck.of("bytea", ByteBuffer.wrap("test6".getBytes()), null)
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
    Function<T, String> overrideToSql;

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

    TypeCheck<T> overrideToSql(Function<T, String> overrideToSql) { this.overrideToSql = overrideToSql; return this; }
    TypeCheck<T> overrideEquals(BiPredicate<T, T> overrideEquals) { this.overrideEquals = overrideEquals; return this; }

    String valToSql(T val) throws Exception {
      if (val == null) return "NULL";
      if (overrideToSql != null) return overrideToSql.apply(val);
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
