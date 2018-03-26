package pgnio;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class CancelTest extends DbTestBase {
  @Test
  public void testCancel() throws Exception {
    // Create a query that lasts for 10 seconds, then open another connection and kill it
    CompletableFuture<QueryReadyConnection.AutoCommit> origConn = Connection.authed(db.conf().dbConf);
    CompletableFuture<List<QueryMessage.Row>> rowsFut = origConn.thenCompose(conn ->
      conn.terminated(conn.simpleQueryRows("SELECT 'test', pg_sleep(10)")));
    origConn.thenCompose(origConnRef ->
        Connection.init(db.conf().dbConf).thenCompose(conn ->
            conn.cancelOther(origConnRef.getProcessId(), origConnRef.getSecretKey()))).get();
    try {
      rowsFut.get();
      Assert.fail();
    } catch (ExecutionException e) {
      DriverException.FromServer serverErr = (DriverException.FromServer) e.getCause();
      Assert.assertEquals("57014", serverErr.notice.getCode());
    }
  }
}
