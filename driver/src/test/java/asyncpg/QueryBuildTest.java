package asyncpg;

import org.junit.Assert;
import org.junit.Test;

public class QueryBuildTest extends DbTestBase {

  @Test
  public void testMultipleBind() {
    withConnectionSync(c ->
        c.simpleQueryExec("CREATE TABLE testMultipleBind (bar VARCHAR(255))").
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
        c.simpleQueryExec("CREATE TABLE testReusePrepared (bar VARCHAR(255))").
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
            thenCompose(rConn ->
                rConn.collectRowCount().
                    thenAccept(count -> Assert.assertEquals(1L, count.longValue())).
                    thenCompose(__ -> rConn.done())).
            thenCompose(conn -> conn.simpleQueryExec("DROP TABLE testReusePrepared"))
    );
  }

  @Test
  public void testReuseBound() {
    // TODO: name a bound, do something in between, then reuse it
  }
}
