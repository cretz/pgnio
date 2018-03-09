package asyncpg;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class SimpleQueryTest extends DbTestBase {
  @Test
  public void testSimpleQuery() throws Exception {
    List<QueryMessage.Row> rows = StartupConnection.init(db.conf().dbConf).thenCompose(sConn ->
        sConn.auth().thenCompose(qConn ->
            qConn.simpleQuery("SELECT current_database() AS database_name;").thenCompose(resConn ->
                resConn.collectRows(Collectors.toList()).thenCompose(res ->
                    resConn.done().thenCompose(Connection::terminate).thenApply(__ -> res)
                )
            )
        )
    ).get();
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(db.conf().dbConf.database, RowReader.DEFAULT.get(rows.get(0), "database_name", String.class));
  }
}
