package asyncpg;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class CopyTest extends DbTestBase {

  @Test
  public void testSimplyCopy() {
    withConnectionSync(c ->
        c.simpleQueryExec("CREATE TABLE testSimplyCopy (foo VARCHAR(255), bar integer)").
            thenCompose(conn -> conn.simpleCopyIn("COPY testSimplyCopy FROM STDIN CSV")).
            thenCompose(copy -> copy.sendData("test1,123\n".getBytes(StandardCharsets.UTF_8))).
            thenCompose(copy -> copy.sendData("test2,456\n".getBytes(StandardCharsets.UTF_8))).
            thenCompose(copy -> copy.done()).
            thenCompose(conn -> conn.simpleQueryRows("SELECT * FROM testSimplyCopy").thenApply(rows -> {
              Assert.assertEquals(2, rows.size());
              Assert.assertEquals("test1", RowReader.DEFAULT.get(rows.get(0), 0, String.class));
              Assert.assertEquals(123, RowReader.DEFAULT.get(rows.get(0), 1, Integer.class).intValue());
              Assert.assertEquals("test2", RowReader.DEFAULT.get(rows.get(1), 0, String.class));
              Assert.assertEquals(456, RowReader.DEFAULT.get(rows.get(1), 1, Integer.class).intValue());
              return conn;
            })).
            thenCompose(conn -> conn.simpleCopyOut("COPY testSimplyCopy TO STDOUT CSV")).
            thenCompose(copy -> {
              StringBuilder bld = new StringBuilder();
              return copy.receiveEachData(bytes -> bld.append(new String(bytes, StandardCharsets.UTF_8)))
                  .thenCompose(__ -> {
                    Assert.assertEquals("test1,123\ntest2,456\n", bld.toString());
                    return copy.done();
                  });
            }).
            thenCompose(conn -> conn.simpleQueryExec("DROP TABLE testSimplyCopy"))
    );
  }
}
