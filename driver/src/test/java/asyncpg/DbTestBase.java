package asyncpg;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public abstract class DbTestBase {

  protected static EmbeddedDb db;

  @BeforeClass
  public static void initDb() {
    db = EmbeddedDb.newFromConfig(new EmbeddedDb.EmbeddedDbConfig().
        dbConf(new Config().
            hostname("localhost").port(5433).database("asyncpg_test").username("some_user").password("some_pass")));
  }

  protected <T> T withConnectionSync(Function<QueryReadyConnection.AutoCommit, CompletableFuture<T>> fn) {
    try {
      return Connection.authed(db.conf().dbConf).thenCompose(conn -> fn.apply(conn).thenCompose(conn::terminate)).get();
    } catch (InterruptedException | ExecutionException e) { throw new RuntimeException(e); }
  }

  @AfterClass
  public static void stopDb() throws Exception {
    db.close();
  }
}
