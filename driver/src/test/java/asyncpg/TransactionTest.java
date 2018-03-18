package asyncpg;

import org.junit.Assert;
import org.junit.Test;

public class TransactionTest extends DbTestBase {
  @Test
  public void testSimpleRollback() {
    withConnectionSync(c ->
        c.simpleQueryExec("CREATE TABLE testSimpleRollback (foo VARCHAR(255))").
            thenCompose(conn -> conn.beginTransaction()).
            thenCompose(tConn -> tConn.simpleQueryExec("INSERT INTO testSimpleRollback VALUES ('test')")).
            thenCompose(tConn -> tConn.rollbackTransaction()).
            thenCompose(conn ->
                conn.simpleQueryRows("SELECT * FROM testSimpleRollback").
                    thenAccept(rows -> Assert.assertEquals(0, rows.size())).
                    thenApply(__ -> conn)).
            thenCompose(conn -> conn.simpleQueryExec("DROP TABLE testSimpleRollback"))
    );
  }

  @Test
  public void testSimpleCommit() {
    withConnectionSync(c ->
        c.simpleQueryExec("CREATE TABLE testSimpleCommit (foo VARCHAR(255))").
            thenCompose(conn -> conn.beginTransaction()).
            thenCompose(tConn -> tConn.simpleQueryExec("INSERT INTO testSimpleCommit VALUES ('test')")).
            thenCompose(tConn -> tConn.commitTransaction()).
            thenCompose(conn ->
                conn.simpleQueryRows("SELECT * FROM testSimpleCommit").
                    thenAccept(rows -> Assert.assertEquals(1, rows.size())).
                    thenApply(__ -> conn)).
            thenCompose(conn -> conn.simpleQueryExec("DROP TABLE testSimpleCommit"))
    );
  }

  @Test
  public void testNestedRollback() {
    // We'll do an start, insert, start again, insert, rollback, insert, commit
    withConnectionSync(c ->
        c.simpleQueryExec("CREATE TABLE testNestedRollback (foo VARCHAR(255))").
            thenCompose(conn -> conn.beginTransaction()).
            thenCompose(tConn -> tConn.simpleQueryExec("INSERT INTO testNestedRollback VALUES ('test')")).
            thenCompose(tConn -> tConn.beginTransaction()).
            thenCompose(tConn -> tConn.simpleQueryExec("INSERT INTO testNestedRollback VALUES ('test2')")).
            thenCompose(tConn -> tConn.rollbackTransaction()).
            thenCompose(tConn -> tConn.simpleQueryExec("INSERT INTO testNestedRollback VALUES ('test3')")).
            thenCompose(tConn -> tConn.commitTransaction()).
            thenCompose(conn -> conn.simpleQueryRows("SELECT * FROM testNestedRollback").thenApply(rows -> {
              Assert.assertEquals(2, rows.size());
              Assert.assertEquals("test", RowReader.DEFAULT.get(rows.get(0), 0, String.class));
              Assert.assertEquals("test3", RowReader.DEFAULT.get(rows.get(1), 0, String.class));
              return conn;
            })).
            thenCompose(conn -> conn.simpleQueryExec("DROP TABLE testNestedRollback"))
    );
  }

  @Test
  public void testNestedCommit() {
    // We'll do a start, insert, start again, insert, commit, insert, rollback
    withConnectionSync(c ->
        c.simpleQueryExec("CREATE TABLE testNestedCommit (foo VARCHAR(255))").
            thenCompose(conn -> conn.beginTransaction()).
            thenCompose(tConn -> tConn.simpleQueryExec("INSERT INTO testNestedCommit VALUES ('test')")).
            thenCompose(tConn -> tConn.beginTransaction()).
            thenCompose(tConn -> tConn.simpleQueryExec("INSERT INTO testNestedCommit VALUES ('test2')")).
            thenCompose(tConn -> tConn.commitTransaction()).
            thenCompose(tConn -> tConn.simpleQueryExec("INSERT INTO testNestedCommit VALUES ('test3')")).
            thenCompose(tConn -> tConn.rollbackTransaction()).
            thenCompose(conn ->
                conn.simpleQueryRows("SELECT * FROM testNestedCommit").
                    thenAccept(rows -> Assert.assertEquals(0, rows.size())).
                    thenApply(__ -> conn)).
            thenCompose(conn -> conn.simpleQueryExec("DROP TABLE testNestedCommit"))
    );
  }
}
