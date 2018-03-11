package asyncpg;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SimpleQueryTest extends DbTestBase {
  @Test
  public void testSimpleQuery() throws Exception {
    List<QueryMessage.Row> rows = Connection.authed(db.conf().dbConf).thenCompose(conn ->
        conn.simpleQueryRows("SELECT current_database() AS database_name;").thenCompose(conn::terminate)).get();
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(db.conf().dbConf.database, RowReader.DEFAULT.get(rows.get(0), "database_name", String.class));
  }
}
