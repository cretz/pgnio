package asyncpg;

import de.flapdoodle.embed.process.config.IRuntimeConfig;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public interface EmbeddedDb extends AutoCloseable {
  // Initially connected to the "postgres" database regardless of conf
  static EmbeddedDb newFromConfig(EmbeddedDbConfig conf) { return new Yandex(conf); }

  EmbeddedDbConfig conf();

  default java.sql.Connection newJdbcConnection() throws SQLException {
    return newJdbcConnection(conf().dbConf.database);
  }

  default java.sql.Connection newJdbcConnection(String overrideDatabase) throws SQLException {
    return DriverManager.getConnection(
        "jdbc:postgresql://" + conf().dbConf.hostname + ":" + conf().dbConf.port + "/" + overrideDatabase,
        conf().dbConf.username, conf().dbConf.password);
  }

  class EmbeddedDbConfig {
    public Config dbConf;
    public List<String> additionalParams =
        Arrays.asList("-E", "SQL_ASCII", "--locale=C", "--lc-collate=C", "--lc-ctype=C");

    public EmbeddedDbConfig dbConf(Config dbConf) { this.dbConf = dbConf; return this; }

    public EmbeddedDbConfig additionalParams(List<String> additionalParams) {
      this.additionalParams = additionalParams;
      return this;
    }
  }

  class Yandex implements EmbeddedDb {
    private final EmbeddedDbConfig conf;
    private final EmbeddedPostgres postgres;
    private final Path dataDir;

    Yandex(EmbeddedDbConfig conf) {
      this.conf = conf;
      try {
        dataDir = Files.createTempDirectory(Files.createDirectories(
            Paths.get(System.getProperty("user.home"), ".embedpostgresql/data")), "pg-data-");
      } catch (IOException e) { throw new RuntimeException(e); }
      postgres = new EmbeddedPostgres(dataDir.toString());
      IRuntimeConfig runtimeConfig = EmbeddedPostgres.cachedRuntimeConfig(
          Paths.get(System.getProperty("user.home"), ".embedpostgresql/extracted"));
      try {
        postgres.start(runtimeConfig, conf.dbConf.hostname, conf.dbConf.port, conf.dbConf.database,
            conf.dbConf.username, conf.dbConf.password, conf.additionalParams);
      } catch (IOException e) { throw new RuntimeException(e); }
    }

    @Override
    public EmbeddedDbConfig conf() { return conf; }

    @Override
    public void close() {
      // TODO: hangs open on non-intellij windows...also, we should delete the data dir here?
      postgres.stop();
    }
  }
}
