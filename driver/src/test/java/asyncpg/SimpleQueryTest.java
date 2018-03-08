package asyncpg;

import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class SimpleQueryTest extends DbTestBase {
  @Test
  public void testSimpleQuery() throws Exception {
    List<QueryMessage.Row> rows =
        StartupConnection.init(db.conf().dbConf).thenCompose(sConn ->
            sConn.
                auth().
                thenCompose(qConn ->
                qConn.simpleQuery("SELECT current_database();").
                    thenCompose(res -> res.collectRows(Collectors.toList())).
                    thenCompose(res -> qConn.terminate().thenApply(__ -> res))).
                whenComplete((__, ___) -> sConn.close())).
            get();
    // TODO: assert contents
    System.out.println("ROWS: " + rows);
  }
}
