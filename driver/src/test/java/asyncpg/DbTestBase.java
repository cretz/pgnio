package asyncpg;

import org.junit.BeforeClass;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public abstract class DbTestBase extends TestBase {
  protected static EmbeddedDb db;

  @BeforeClass
  public synchronized static void initDb() {
    if (db == null) {
      db = EmbeddedDb.newFromConfig(new EmbeddedDb.EmbeddedDbConfig().
          dbConf(new Config().
              hostname("localhost").port(5433).database("asyncpg_test").username("some_user").password("some_pass")));
      // Add a shutdown hook to close it
      Runtime.getRuntime().addShutdownHook(new Thread(db::close));
    }
  }

  protected <T> T withConnectionSync(Function<QueryReadyConnection.AutoCommit, CompletableFuture<T>> fn) {
    try {
      return Connection.authed(db.conf().dbConf).thenCompose(conn -> conn.terminated(fn.apply(conn))).get();
    } catch (InterruptedException | ExecutionException e) { throw new RuntimeException(e); }
  }
}
