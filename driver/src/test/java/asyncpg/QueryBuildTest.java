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
            thenCompose(conn -> conn.reusePrepared("testReuseBound-query")).
            thenCompose(pConn -> pConn.reuseBound("testReuseBound-bound")).
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

  // TODO: test execute partial
}
