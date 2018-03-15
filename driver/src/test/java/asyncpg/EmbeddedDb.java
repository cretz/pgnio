package asyncpg;

import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.distribution.Platform;
import de.flapdoodle.embed.process.runtime.ICommandLinePostProcessor;
import de.flapdoodle.embed.process.store.IArtifactStore;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public interface EmbeddedDb {
  // Initially connected to the "postgres" database regardless of conf
  static EmbeddedDb newFromConfig(EmbeddedDbConfig conf) { return new Yandex(conf); }

  EmbeddedDbConfig conf();

  void close();

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
    public List<String> additionalInitDbParams =
        Arrays.asList("-E", "SQL_ASCII", "--locale=C", "--lc-collate=C", "--lc-ctype=C");

    public EmbeddedDbConfig dbConf(Config dbConf) { this.dbConf = dbConf; return this; }

    public EmbeddedDbConfig additionalInitDbParams(List<String> additionalInitDbParams) {
      this.additionalInitDbParams = additionalInitDbParams;
      return this;
    }
  }

  class Yandex implements EmbeddedDb {
    private static final Logger log = Logger.getLogger(Yandex.class.getName());

    private final EmbeddedDbConfig conf;
    private final EmbeddedPostgres postgres;
    private final Path dataDir;

    Yandex(EmbeddedDbConfig conf) {
      this.conf = conf;
      try {
        dataDir = Files.createTempDirectory(Files.createDirectories(
            Paths.get(System.getProperty("user.home"), ".embedpostgresql/data")), "pg-data-");
      } catch (Exception e) { throw new RuntimeException(e); }
      postgres = new EmbeddedPostgres(dataDir.toString());
      IRuntimeConfig runtimeConfig = new DelegatingCommandLineOverrideRuntimeConfig(
          dataDir, EmbeddedPostgres.cachedRuntimeConfig(
              Paths.get(System.getProperty("user.home"), ".embedpostgresql/extracted")),
          "-l");
      try {
        postgres.start(runtimeConfig, conf.dbConf.hostname, conf.dbConf.port, conf.dbConf.database,
            conf.dbConf.username, conf.dbConf.password, conf.additionalInitDbParams);
      } catch (IOException e) { throw new RuntimeException(e); }
    }

    @Override
    public EmbeddedDbConfig conf() { return conf; }

    @Override
    public void close() {
      // TODO: hangs open on non-intellij windows
      postgres.stop();
      try {
        Files.walk(dataDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      } catch (IOException e) { Logger.getAnonymousLogger().log(Level.WARNING, "Unable to delete " + dataDir, e); }
    }

    public static class DelegatingCommandLineOverrideRuntimeConfig
        implements IRuntimeConfig, ICommandLinePostProcessor {
      public static final String POSTGRES_EXE_NAME =
          Platform.detect().equals(Platform.Windows) ? "postgres.exe" : "postgres";
      public final Path dataDir;
      public final IRuntimeConfig delegate;
      public final List<String> additionalArgs;
      public DelegatingCommandLineOverrideRuntimeConfig(
          Path dataDir, IRuntimeConfig delegate, String... additionalArgs) {
        this.dataDir = dataDir;
        this.delegate = delegate;
        this.additionalArgs = Arrays.asList(additionalArgs);
      }

      @Override
      public ProcessOutput getProcessOutput() { return delegate.getProcessOutput(); }

      @Override
      public IArtifactStore getArtifactStore() { return delegate.getArtifactStore(); }

      @Override
      public boolean isDaemonProcess() { return delegate.isDaemonProcess(); }

      @Override
      public ICommandLinePostProcessor getCommandLinePostProcessor() { return this; }

      @Override
      public List<String> process(Distribution distribution, List<String> args) {
        List<String> result = delegate.getCommandLinePostProcessor().process(distribution, args);
        if (!result.isEmpty() && result.get(0).endsWith(POSTGRES_EXE_NAME)) {
          // Add SSL keys
          try {
            Files.write(dataDir.resolve("server.crt"),
                Files.readAllBytes(Paths.get(getClass().getResource("keys/server.crt").toURI())));
            Files.write(dataDir.resolve("server.key"),
                Files.readAllBytes(Paths.get(getClass().getResource("keys/server.key").toURI())));
          } catch (Exception e) { throw new RuntimeException(e); }
          result.addAll(additionalArgs);
        }
        log.log(Level.INFO, "Running: " + String.join(" ", result));
        return result;
      }
    }
  }
}
