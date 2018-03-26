package pgnio;

import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.distribution.IVersion;
import de.flapdoodle.embed.process.distribution.Platform;
import de.flapdoodle.embed.process.runtime.ICommandLinePostProcessor;
import de.flapdoodle.embed.process.store.IArtifactStore;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;
import ru.yandex.qatools.embed.postgresql.distribution.Version;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public interface EmbeddedDb {
  // Initially connected to the "postgres" database regardless of conf
  static EmbeddedDb newFromConfig(EmbeddedDbConfig conf) { return new Yandex(conf); }

  EmbeddedDbConfig conf();

  void close();

  int majorVersion();
  int minorVersion();

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
    public List<String> startupQueries = Collections.singletonList("CREATE EXTENSION hstore;");

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
    private final IVersion version;

    Yandex(EmbeddedDbConfig conf) {
      this.conf = conf;
      try {
        dataDir = Files.createTempDirectory(Files.createDirectories(
            Paths.get(System.getProperty("user.home"), ".embedpostgresql/data")), "pg-data-");
      } catch (Exception e) { throw new RuntimeException(e); }
      IVersion version = Version.Main.PRODUCTION;
      if (System.getProperty("pgnio.postgres.version") != null &&
          !System.getProperty("pgnio.postgres.version").isEmpty()) {
        version = () -> System.getProperty("pgnio.postgres.version");
      }
      this.version = version;
      // Just make sure major and minor versions can parse
      majorVersion();
      minorVersion();
      postgres = new EmbeddedPostgres(version, dataDir.toString());
      IRuntimeConfig runtimeConfig = new DelegatingCommandLineOverrideRuntimeConfig(
          dataDir, EmbeddedPostgres.cachedRuntimeConfig(
              Paths.get(System.getProperty("user.home"), ".embedpostgresql/extracted")),
          "-l");
      try {
        postgres.start(runtimeConfig, conf.dbConf.hostname, conf.dbConf.port, conf.dbConf.database,
            conf.dbConf.username, conf.dbConf.password, conf.additionalInitDbParams);
      } catch (IOException e) { throw new RuntimeException(e); }
      // If there are startup queries, run those
      if (!conf.startupQueries.isEmpty()) {
        try (java.sql.Connection conn = newJdbcConnection()) {
          try (Statement stmt = conn.createStatement()) {
            for (String startupQuery : conf.startupQueries) {
              log.log(Level.FINE, "Running startup query: {0}", startupQuery);
              stmt.execute(startupQuery);
            }
          }
        } catch (SQLException e) { throw new RuntimeException(e); }
      }
    }

    @Override
    public EmbeddedDbConfig conf() { return conf; }

    @Override
    public void close() {
      log.log(Level.WARNING, "Stopping Postgres");
      try {
        postgres.stop();
      } catch (Exception e) { log.log(Level.WARNING, "Failed to stop Postgres", e); }
      try {
        Files.walk(dataDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(file -> {
          if (!file.delete()) log.log(Level.WARNING, "Failed to delete {0}", file);
        });
      } catch (IOException e) { log.log(Level.WARNING, "Failed to delete " + dataDir, e); }
    }

    @Override
    public int majorVersion() {
      int dotIndex = version.asInDownloadPath().indexOf('.');
      if (dotIndex == -1) throw new IllegalArgumentException("Invalid version: " + version.asInDownloadPath());
      return Integer.parseInt(version.asInDownloadPath().substring(0, dotIndex));
    }

    @Override
    public int minorVersion() {
      int dotIndex = version.asInDownloadPath().indexOf('.');
      if (dotIndex == -1) throw new IllegalArgumentException("Invalid version: " + version.asInDownloadPath());
      int minorEndIndex = version.asInDownloadPath().indexOf('.', dotIndex + 1);
      if (minorEndIndex == -1) minorEndIndex = version.asInDownloadPath().indexOf('-', dotIndex + 1);
      if (minorEndIndex == -1) throw new IllegalArgumentException("Invalid version: " + version.asInDownloadPath());
      return Integer.parseInt(version.asInDownloadPath().substring(dotIndex + 1, minorEndIndex));
    }

    public static class DelegatingCommandLineOverrideRuntimeConfig
        implements IRuntimeConfig, ICommandLinePostProcessor {
      public static final String POSTGRES_EXE_CONTAINS =
          Platform.detect().equals(Platform.Windows) ? "bin\\postgres.exe" : "bin/postgres";
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
        List<String> result = new ArrayList<>(delegate.getCommandLinePostProcessor().process(distribution, args));
        if (!result.isEmpty() && result.stream().anyMatch(s -> s.contains(POSTGRES_EXE_CONTAINS))) {
          // Add SSL keys
          try {
            Files.write(dataDir.resolve("server.crt"),
                Files.readAllBytes(Paths.get(getClass().getResource("keys/server.crt").toURI())));
            Files.write(dataDir.resolve("server.key"),
                Files.readAllBytes(Paths.get(getClass().getResource("keys/server.key").toURI())));
          } catch (Exception e) { throw new RuntimeException(e); }
          // As a special case, if this is "runas" on Windows then we have to
          //  add these as string pieces inside the last quote
          if (result.get(0).equals("runas") && result.get(result.size() - 1).endsWith("\"")) {
            String lastParam = result.get(result.size() - 1);
            lastParam = lastParam.substring(0, lastParam.length() - 1) + ' ' + String.join(" ", additionalArgs) + '"';
            result.set(result.size() - 1, lastParam);
          } else {
            result.addAll(additionalArgs);
          }
        }
        log.log(Level.INFO, "Running: " + String.join(" ", result));
        return result;
      }
    }
  }
}
