package asyncpg;

import org.junit.Assert;
import org.junit.Test;

public class QueryBuildTest extends DbTestBase {
  @Test
  public void testMultipleBind() {
    withConnectionSync(c ->
        c.simpleQueryExec("CREATE TABLE testMultipleBind (foo VARCHAR(255))").
            thenCompose(conn -> conn.prepare("INSERT INTO testMultipleBind VALUES ($1)")).
                thenCompose(pConn -> pConn.bindExecuteAndBack("test1")).
                thenCompose(pConn -> pConn.bindExecuteAndDone("test2")).
                thenCompose(rConn ->
                    rConn.collectRowCount().
                        // Has two but not a third
                        thenAccept(count -> Assert.assertEquals(1L, count.longValue())).
                        thenCompose(__ -> rConn.collectRowCount()).
                        thenAccept(count -> Assert.assertEquals(1L, count.longValue())).
                        thenCompose(__ -> rConn.collectRowCount()).
                        thenAccept(count -> Assert.assertNull(count)).
                        thenCompose(__ -> rConn.done())).
                thenCompose(conn -> conn.simpleQueryExec("DROP TABLE testMultipleBind"))
    );
  }

  @Test
  public void testReusePrepared() {
    withConnectionSync(c ->
        c.simpleQueryExec("CREATE TABLE testReusePrepared (foo VARCHAR(255))").
            thenCompose(conn ->
                conn.prepareReusable("testReusePrepared-query", "INSERT INTO testReusePrepared VALUES ($1)")).
            thenCompose(pConn -> pConn.done()).
            thenCompose(rConn -> rConn.done()).
            thenCompose(conn ->
                conn.simpleQueryRows("SELECT * FROM testReusePrepared").
                    thenAccept(rows -> Assert.assertTrue(rows.isEmpty())).
                    thenApply(__ -> conn)).
            thenCompose(conn -> conn.reusePrepared("testReusePrepared-query")).
            thenCompose(pConn -> pConn.bindExecuteAndDone("test1")).
            thenCompose(rConn -> rConn.done()).
            thenCompose(conn ->
                conn.simpleQueryRows("SELECT * FROM testReusePrepared").
                    thenAccept(rows -> Assert.assertEquals(1, rows.size())).
                    thenApply(__ -> conn)).
            thenCompose(conn -> conn.simpleQueryExec("DROP TABLE testReusePrepared"))
    );
  }

  @Test
  public void testReuseBound() {
    withConnectionSync(c ->
        c.simpleQueryExec("CREATE TABLE testReuseBound (foo VARCHAR(255))").
            thenCompose(conn -> conn.beginTransaction()).
            thenCompose(conn ->
                conn.prepareReusable("testReuseBound-query", "INSERT INTO testReuseBound VALUES ($1)")).
            thenCompose(pConn -> pConn.bindReusable("testReuseBound-bound", "test")).
            thenCompose(bConn -> bConn.done()).
            thenCompose(rConn -> rConn.done()).
            thenCompose(conn ->
                conn.simpleQueryRows("SELECT * FROM testReuseBound").
                    thenAccept(rows -> Assert.assertEquals(0, rows.size())).
                    thenApply(__ -> conn)).
            thenCompose(conn -> conn.reuseBound("testReuseBound-bound")).
            thenCompose(bConn -> bConn.executeAndDone()).
            thenCompose(rConn -> rConn.done()).
            thenCompose(conn -> conn.commitTransaction()).
            thenCompose(conn ->
                conn.simpleQueryRows("SELECT * FROM testReuseBound").
                    thenAccept(rows -> Assert.assertEquals(1, rows.size())).
                    thenApply(__ -> conn)).
            thenCompose(conn -> conn.simpleQueryExec("DROP TABLE testReuseBound"))
    );
  }

  @Test
  public void testBoundExecuteMax() {
    // Grab some, then go back and grab the rest
    StringBuilder inserts = new StringBuilder();
    for (int i = 0; i < 50; i++) inserts.append("\nINSERT INTO testBoundExecuteMax VALUES(" + i + ");");
    withConnectionSync(c ->
        c.simpleQueryExec("CREATE TABLE testBoundExecuteMax (foo INTEGER); " + inserts).
            thenCompose(conn -> conn.beginTransaction()).
            thenCompose(conn -> conn.prepare("SELECT * FROM testBoundExecuteMax ORDER BY foo")).
            thenCompose(pConn -> pConn.bindReusable("testBoundExecuteMax-bound")).
            thenCompose(bConn -> bConn.describe()).
            thenCompose(bConn -> bConn.execute(20)).
            thenCompose(bConn -> bConn.done()).
            thenCompose(rConn ->
                rConn.collectRows().thenCompose(rows -> {
                  Assert.assertTrue(rConn.isSuspended());
                  Assert.assertEquals(20, rows.size());
                  for (int i = 0; i < rows.size(); i++)
                    Assert.assertEquals(i, RowReader.DEFAULT.get(rows.get(i), "foo", Integer.class).intValue());
                  return rConn.done();
                })).
            thenCompose(conn -> conn.reuseBound("testBoundExecuteMax-bound")).
            thenCompose(bConn -> bConn.describeExecuteAndDone()).
            thenCompose(rConn ->
                rConn.collectRows().thenCompose(rows -> {
                  Assert.assertFalse(rConn.isSuspended());
                  Assert.assertEquals(30, rows.size());
                  for (int i = 0; i < rows.size(); i++)
                    Assert.assertEquals(i + 20, RowReader.DEFAULT.get(rows.get(i), 0, Integer.class).intValue());
                  return rConn.done();
                })).
            thenCompose(conn -> conn.commitTransaction()).
            thenCompose(conn -> conn.simpleQueryExec("DROP TABLE testBoundExecuteMax"))
    );
  }
}
