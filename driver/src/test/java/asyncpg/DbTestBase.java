package asyncpg;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class DbTestBase {

  protected static EmbeddedDb db;

  @BeforeClass
  public static void initDb() {
    db = EmbeddedDb.newFromConfig(new EmbeddedDb.EmbeddedDbConfig().
        dbConf(new Config().
            hostname("localhost").port(5433).database("pagila").username("some_user").password("some_pass")));
  }

  @AfterClass
  public static void stopDb() throws Exception {
    db.close();
  }
}
