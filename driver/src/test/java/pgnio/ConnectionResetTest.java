package pgnio;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionResetTest extends DbTestBase {
  private static final Logger log = Logger.getLogger(ConnectionResetTest.class.getName());

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
    assertConnectionCanResetAfter(c ->
        c.beginTransaction().
            thenCompose(conn -> conn.simpleQuery("SELECT * FROM generate_series(1, 4)")));
  }

  @Test
  public void testInNestedTransaction() {
    assertConnectionCanResetAfter(c ->
        c.beginTransaction().
            thenCompose(conn -> conn.beginTransaction()).
            thenCompose(conn -> conn.simpleQuery("SELECT * FROM generate_series(1, 4)")));
  }

  @Test
  public void testPrepared() {
    assertConnectionCanResetAfter(c ->
        c.prepare("SELECT * FROM generate_series(1, $1)").
            thenCompose(pConn -> pConn.describe()));
  }

  @Test
  public void testBound() {
    assertConnectionCanResetAfter(c ->
        c.prepare("SELECT * FROM generate_series(1, $1)").
            thenCompose(pConn -> pConn.bind(4)).
            thenCompose(bConn -> bConn.describe()));
  }

  @Test
  public void testBoundAndExecuted() {
    assertConnectionCanResetAfter(c ->
        c.prepare("SELECT * FROM generate_series(1, $1)").
            thenCompose(pConn -> pConn.bind(4)).
            thenCompose(bConn -> bConn.execute()));
  }

  @Test
  public void testBoundMaxRows() {
    assertConnectionCanResetAfter(c ->
        c.prepare("SELECT * FROM generate_series(1, $1)").
            thenCompose(pConn -> pConn.bindReusable("testBoundMaxRows-bound", 4)).
            thenCompose(bConn -> bConn.execute(2)));
  }

  @Test
  public void testBoundMaxRowsSuspended() {
    assertConnectionCanResetAfter(c ->
        c.prepare("SELECT * FROM generate_series(1, $1)").
            thenCompose(pConn -> pConn.bindReusable("testBoundMaxRowsSuspended-bound", 4)).
            thenCompose(bConn -> bConn.execute(2)).
            thenCompose(bConn -> bConn.done()));
  }

  @Test
  public void testBoundMaxRowsSuspendedResultDone() {
    assertConnectionCanResetAfter(c ->
        c.prepare("SELECT * FROM generate_series(1, $1)").
            thenCompose(pConn -> pConn.bindReusable("testBoundMaxRowsSuspendedResultDone-bound", 4)).
            thenCompose(bConn -> bConn.execute(2)).
            thenCompose(bConn -> bConn.done()).
            thenCompose(rConn -> rConn.done()));
  }

  @Test
  public void testSomeRowsRead() {
    assertConnectionCanResetAfter(c ->
        c.simpleQuery("SELECT * FROM generate_series(1, 4)").
            thenCompose(rConn -> rConn.next().thenApply(__ -> rConn)).
            thenCompose(rConn -> rConn.next()));
  }

  @Test
  public void testMultipleQueriesSomeRun() {
    assertConnectionCanResetAfter(c ->
        c.simpleQuery("SELECT * FROM generate_series(1, 4); " +
            "SELECT * FROM generate_series(5, 8); SELECT * FROM generate_series(9, 12);").
            thenCompose(rConn -> rConn.collectRows()));
  }

  @Test
  public void testErroredPrepare() {
    assertConnectionCanResetAfter(c ->
        c.prepare("SELECT * FROM does_not_exist"));
  }

  @Test
  public void testErroredBind() {
    assertConnectionCanResetAfter(c ->
        c.prepare("SELECT * FROM generate_series(1, $1)").
            thenCompose(pConn -> pConn.bind("test")).
            thenCompose(bConn -> bConn.done()));
  }

  @Test
  public void testErroredExecute() {
    assertConnectionCanResetAfter(c ->
        c.prepare("SELECT * FROM generate_series(1, $1)").
            thenCompose(pConn -> pConn.bindExecuteAndDone("test")));
  }

  @Test
  public void testMidCopyInCopyMode() {
    assertConnectionCanResetAfter(c ->
        c.simpleQueryExec("CREATE TEMP TABLE testMidCopyInCopyMode (foo VARCHAR(255), bar integer)").
            thenCompose(conn -> conn.simpleCopyIn("COPY testMidCopyInCopyMode FROM STDIN CSV")).
            thenCompose(copy -> copy.sendData("test1".getBytes(StandardCharsets.UTF_8))));
  }

  @Test
  public void testMidCopyInResultMode() {
    assertConnectionCanResetAfter(c ->
        c.simpleQueryExec("CREATE TEMP TABLE testMidCopyInResultMode (foo VARCHAR(255), bar integer)").
            thenCompose(conn -> conn.simpleQuery("COPY testMidCopyInResultMode FROM STDIN CSV")).
            thenCompose(rConn -> rConn.sendCopyData("test1".getBytes(StandardCharsets.UTF_8))));
  }

  @Test
  public void testMidCopyOutCopyMode() {
    assertConnectionCanResetAfter(c ->
        c.simpleQueryExec("CREATE TEMP TABLE testMidCopyOutCopyMode (foo VARCHAR(255), bar integer);" +
            "INSERT INTO testMidCopyOutCopyMode VALUES ('test1', 1);" +
            "INSERT INTO testMidCopyOutCopyMode VALUES ('test2', 1);" +
            "INSERT INTO testMidCopyOutCopyMode VALUES ('test3', 3);").
            thenCompose(conn -> conn.simpleCopyOut("COPY testMidCopyOutCopyMode TO STDOUT CSV")).
            thenCompose(copy -> copy.receiveData()));
  }

  @Test
  public void testMidCopyOutResultMode() {
    assertConnectionCanResetAfter(c ->
        c.simpleQueryExec("CREATE TEMP TABLE testMidCopyOutResultMode (foo VARCHAR(255), bar integer);" +
            "INSERT INTO testMidCopyOutResultMode VALUES ('test1', 1);" +
            "INSERT INTO testMidCopyOutResultMode VALUES ('test2', 1);" +
            "INSERT INTO testMidCopyOutResultMode VALUES ('test3', 3);").
            thenCompose(conn -> conn.simpleQuery("COPY testMidCopyOutResultMode TO STDOUT CSV")));
  }

  @Test
  public void testErroredCopyInCopyMode() {
    assertConnectionCanResetAfter(c ->
        c.simpleQueryExec("CREATE TEMP TABLE testErroredCopyInCopyMode (foo VARCHAR(255), bar integer)").
            thenCompose(conn -> conn.simpleCopyIn("COPY testErroredCopyInCopyMode FROM STDIN CSV")).
            thenCompose(copy -> copy.sendData("test1,test2".getBytes(StandardCharsets.UTF_8))));
  }

  @Test
  public void testErroredCopyInResultMode() {
    assertConnectionCanResetAfter(c ->
        c.simpleQueryExec("CREATE TEMP TABLE testErroredCopyInResultMode (foo VARCHAR(255), bar integer)").
            thenCompose(conn -> conn.simpleQuery("COPY testErroredCopyInResultMode FROM STDIN CSV")).
            thenCompose(rConn -> rConn.sendCopyData("test1,test2".getBytes(StandardCharsets.UTF_8))));
  }

  @Test
  public void testSendCopyWhenNotCopying() {
    assertConnectionCanResetAfter(c ->
        c.simpleQuery("SELECT 'test1'").
            thenCompose(rConn -> rConn.sendCopyData("test1,test2".getBytes(StandardCharsets.UTF_8))));
  }

  @Test
  public void testCancelledQuery() {
    assertConnectionCanResetAfter(c -> {
      CompletableFuture<?> fut = c.simpleQueryRows("SELECT 'test', pg_sleep(10)");
      try {
        Thread.sleep(50);
        Connection.init(db.conf().dbConf).thenCompose(conn ->
            conn.cancelOther(c.getProcessId(), c.getSecretKey())).get();
      } catch (Exception e) { throw new RuntimeException(e); }
      return fut;
    });
  }

  void assertConnectionCanResetAfter(Function<QueryReadyConnection.AutoCommit, CompletableFuture<?>> fn) {
    // Let's make sure it can query
    withConnectionSync(origConn ->
        fn.apply(origConn).
            handle((__, ex) -> {
              if (ex != null) log.log(Level.FINE, "Ignoring error", ex);
              return origConn;
            }).
            thenCompose(Connection.Started::fullReset).
            thenCompose(conn -> conn.simpleQueryRows("SELECT 'test'")).
            thenAccept(rows -> Assert.assertEquals("test", RowReader.DEFAULT.get(rows.get(0), 0, String.class))));
    // Let's also make sure it can error expectedly
    withConnectionSync(origConn ->
        fn.apply(origConn).
            handle((__, ex) -> {
              if (ex != null) log.log(Level.FINE, "Ignoring error", ex);
              return origConn;
            }).
            thenCompose(Connection.Started::fullReset).
            thenCompose(conn -> conn.simpleQueryExec("SELECT * FROM not_really_there")).
            handle((__, ex) -> {
              Assert.assertTrue(ex.getMessage().contains("\"not_really_there\""));
              return null;
            }));
  }
}
