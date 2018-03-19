package asyncpg;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class ConnectionResetTest extends DbTestBase {

  @Test
  public void testBadQuery() {
    assertConnectionCanResetAfter(conn -> conn.simpleQueryExec("SELECT does not exist bad sql"));
  }

  @Test
  public void testAfterQuery() {
    assertConnectionCanResetAfter(conn -> conn.simpleQuery("SELECT 'foo'"));
  }

  @Test
  public void testInTransaction() {
    // TODO
  }

  @Test
  public void testInNestedTransaction() {
    // TODO
  }

  @Test
  public void testPrepared() {
    // TODO
  }

  @Test
  public void testBound() {
    // TODO
  }

  @Test
  public void testBoundAndExecuted() {
    // TODO
  }

  @Test
  public void testBoundMaxRowsSuspended() {
    // TODO
  }

  @Test
  public void testSomeRowsRead() {
    // TODO
  }

  @Test
  public void testMultipleQueriesSomeRun() {
    // TODO
  }

  @Test
  public void testErroredPrepare() {
    // TODO
  }

  @Test
  public void testErroredBind() {
    // TODO
  }

  @Test
  public void testErroredExecute() {
    // TODO
  }

  @Test
  public void testMidCopyIn() {
    // TODO
  }

  @Test
  public void testMidCopyOut() {
    // TODO
  }

  @Test
  public void testErroredCopyIn() {
    // TODO
  }

  @Test
  public void testCancelledQuery() {
    // TODO
  }

  void assertConnectionCanResetAfter(Function<QueryReadyConnection.AutoCommit, CompletableFuture<?>> fn) {
    withConnectionSync(origConn ->
      fn.apply(origConn).
          handle((__, ___) -> origConn).
          thenCompose(Connection.Started::fullReset).
          thenCompose(conn -> conn.simpleQueryRows("SELECT 'test'")).
          thenAccept(rows -> Assert.assertEquals("test", RowReader.DEFAULT.get(rows.get(0), 0, String.class))));
  }
}
