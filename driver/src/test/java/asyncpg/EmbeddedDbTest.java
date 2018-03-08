package asyncpg;

import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class EmbeddedDbTest extends DbTestBase {
  @Test
  public void testSimpleJdbc() throws SQLException {
    try (java.sql.Connection conn = db.newJdbcConnection()) {
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SELECT current_database();")) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(db.conf().dbConf.database, rs.getString(1));
        }
      }
    }
  }
}
