package pgnio;

import org.junit.BeforeClass;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public abstract class DbTestBase extends TestBase {
  protected static EmbeddedDb db;

  @BeforeClass
  public synchronized static void initDb() {
    if (db == null) {
      db = EmbeddedDb.newFromConfig(new EmbeddedDb.EmbeddedDbConfig().dbConf(newDefaultConfig()));
      // Add a shutdown hook to close it
      Runtime.getRuntime().addShutdownHook(new Thread(db::close));
    }
  }

  protected static Config newDefaultConfig() {
    return new Config().hostname("localhost").port(5433).
        database("pgnio_test").username("some_user").password("some_pass").defaultTimeout(10, TimeUnit.SECONDS);
  }

  protected <T> T withConnectionSync(Function<QueryReadyConnection.AutoCommit, CompletableFuture<T>> fn) {
    return withConnectionSync(db.conf().dbConf, fn);
  }

  protected <T> T withConnectionSync(Config conf, Function<QueryReadyConnection.AutoCommit, CompletableFuture<T>> fn) {
    try {
      return Connection.authed(conf).thenCompose(conn -> conn.terminated(fn.apply(conn))).get();
    } catch (InterruptedException | ExecutionException e) { throw new RuntimeException(e); }
  }
}
