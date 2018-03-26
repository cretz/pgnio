package pgnio;

import org.junit.Assert;
import org.junit.Test;

public class ConnectionPoolTest extends DbTestBase {

  @Test
  public void testSamePidInPool() throws Exception {
    ConnectionPool pool = new ConnectionPool(newDefaultConfig().poolSize(1));
    int pid = pool.withConnection(c ->
        // Could use the process ID in the connection, but might as well ask for it
        c.simpleQueryRows("SELECT pg_backend_pid()").thenApply(rows ->
            RowReader.DEFAULT.get(rows.get(0), 0, Integer.class))).get();
    int nextPid = pool.withConnection(c ->
        c.simpleQueryRows("SELECT pg_backend_pid()").thenApply(rows ->
            RowReader.DEFAULT.get(rows.get(0), 0, Integer.class))).get();
    Assert.assertEquals(pid, nextPid);
  }

  @Test
  public void testDifferentPidAfterSelfTerminated() throws Exception {
    ConnectionPool pool = new ConnectionPool(newDefaultConfig().poolSize(1));
    int pid = pool.withConnection(c ->
        c.terminated(c.simpleQueryRows("SELECT pg_backend_pid()").thenApply(rows ->
            RowReader.DEFAULT.get(rows.get(0), 0, Integer.class)))).get();
    int nextPid = pool.withConnection(c ->
        c.simpleQueryRows("SELECT pg_backend_pid()").thenApply(rows ->
            RowReader.DEFAULT.get(rows.get(0), 0, Integer.class))).get();
    Assert.assertNotEquals(pid, nextPid);
  }

  @Test
  public void testDifferentPidAfterExternallyTerminated() throws Exception {
    ConnectionPool pool = new ConnectionPool(newDefaultConfig().poolSize(1).poolValidationQuery("SELECT 1"));
    int pid = pool.withConnection(c ->
        c.simpleQueryRows("SELECT pg_backend_pid()").thenApply(rows ->
            RowReader.DEFAULT.get(rows.get(0), 0, Integer.class))).get();
    withConnectionSync(c -> c.simpleQueryExec("SELECT pg_terminate_backend(" + pid + ")"));
    int nextPid = pool.withConnection(c ->
        c.simpleQueryRows("SELECT pg_backend_pid()").thenApply(rows ->
            RowReader.DEFAULT.get(rows.get(0), 0, Integer.class))).get();
    Assert.assertNotEquals(pid, nextPid);
  }
}
