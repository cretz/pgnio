package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Configuration for connection and connection pools. While these are stored as public mutable fields, in general they
 * should not be mutated once they are in use. Doing so may result in undefined behavior.
 */
public class Config {
  /** The hostname to connect to. Default 'localhost' */
  public String hostname = "localhost";
  /** The port to connect to. Default '5432' */
  public int port = 5432;
  /** The username to connect with. Default 'postgres' */
  public String username = "postgres";
  /** The password to connect with. May be null (the default) */
  public @Nullable String password;
  /** the database to connect to. May be null (the default) */
  public @Nullable String database;
  /**
   * Whether to support/require SSL. If false (the default), SSL will not even be attempted. If true, SSL is required.
   * If null, SSL will be tried but will fallback to insecure if not available.
   */
  public @Nullable Boolean ssl = false;
  /**
   * The default timeout in {@link #defaultTimeoutUnit}s for all async reads and writes. 0 means no timeout (the
   * default)
   */
  public long defaultTimeout;
  /** The unit for {@link #defaultTimeout}. Default {@link TimeUnit#MILLISECONDS} */
  public TimeUnit defaultTimeoutUnit = TimeUnit.MILLISECONDS;
  /** Whether to use "direct" byte buffers for building reads/writes */
  public boolean directBuffer = true;
  /** The amount of bytes to add when the buffers need to grow. Default '1000' */
  public int bufferStep = 1000;
  /** The internal Postgres protocol version in use. Default '196608' */
  public int protocolVersion = 196608;
  /** Additional Postgres parameters for connections. Null (the default) is the same as empty */
  public @Nullable Map<String, String> additionalStartupParams;
  /** If true (the default), notices are logged via {@link Subscribable.Notice#log(Logger, Connection.Context)} */
  public boolean logNotices = true;
  /** If true (the default), Postgres text format is the format used when communicating */
  public boolean preferText = true;
  /** The {@link ParamWriter} to use when converting query parameter objects to protocol values */
  public ParamWriter paramWriter = ParamWriter.DEFAULT;
  /** The number of connections maintained in the {@link ConnectionPool} (when used) */
  public int poolSize = 5;
  /**
   * If present, the query that is run by the connection pool to make sure the connection is valid before returning it.
   * Default is null
   */
  public @Nullable String poolValidationQuery;
  /** Function to connect and authenticate a connection from a config. Default is {@link Connection#authed(Config)} */
  @SuppressWarnings("initialization")
  public Function<Config, CompletableFuture<QueryReadyConnection.AutoCommit>> connector = Connection::authed;
  /**
   * Function to establish a {@link ConnectionIo} connection from a config. Default is
   * {@link ConnectionIo.AsyncSocketChannel#connect(Config)}
   */
  @SuppressWarnings("initialization")
  public Function<Config, CompletableFuture<? extends ConnectionIo>> ioConnector =
      ConnectionIo.AsyncSocketChannel::connect;
  /**
   * {@link SSLContext} that is used by the default {@link #sslWrapper} call to obtain an SSL connection. The context
   * must be "initialized".
   */
  public @Nullable SSLContext sslContextOverride;
  /**
   * Function to start SSL over a {@link ConnectionIo} and return a new one secure and ready to use. The default uses
   * {@link #sslContextOverride} if present or {@link SSLContext#getDefault()} otherwise. It uses
   * {@link ConnectionIo.Ssl} to wrap the insecure IO.
   */
  @SuppressWarnings("initialization")
  public Function<ConnectionIo, CompletableFuture<? extends ConnectionIo>> sslWrapper = (io) -> {
    try {
      SSLContext ctx = sslContextOverride == null ? SSLContext.getDefault() : sslContextOverride;
      ConnectionIo.Ssl sslIo = new ConnectionIo.Ssl(io, ctx.createSSLEngine(),
          directBuffer, defaultTimeout, defaultTimeoutUnit);
      return sslIo.start().thenApply(__ -> sslIo);
    } catch (NoSuchAlgorithmException e) { throw new RuntimeException(e); }
  };

  /** @see #hostname */
  public Config hostname(String hostname) { this.hostname = hostname; return this; }
  /** @see #port */
  public Config port(int port) { this.port = port; return this; }
  /** @see #username */
  public Config username(String username) { this.username = username; return this; }
  /** @see #password */
  public Config password(String password) { this.password = password; return this; }
  /** @see #database */
  public Config database(String database) { this.database = database; return this; }
  /** @see #ssl */
  public Config ssl(@Nullable Boolean ssl) { this.ssl = ssl; return this; }
  /**
   * @see #defaultTimeout
   * @see #defaultTimeoutUnit
   */
  public Config defaultTimeout(long defaultTimeout, TimeUnit defaultTimeoutUnit) {
    this.defaultTimeout = defaultTimeout;
    this.defaultTimeoutUnit = defaultTimeoutUnit;
    return this;
  }
  /** @see #directBuffer */
  public Config directBuffer(boolean directBuffer) { this.directBuffer = directBuffer; return this; }
  /** @see #bufferStep */
  public Config bufferStep(int bufferStep) { this.bufferStep = bufferStep; return this; }
  /** @see #protocolVersion */
  public Config protocolVersion(int protocolVersion) { this.protocolVersion = protocolVersion; return this; }
  /** @see #additionalStartupParams */
  public Config additionalStartupParams(Map<String, String> additionalStartupParams) {
    this.additionalStartupParams = additionalStartupParams;
    return this;
  }
  /** @see #logNotices */
  public Config logNotices(boolean logNotices) { this.logNotices = logNotices; return this; }
  /** @see #preferText */
  public Config preferText(boolean preferText) { this.preferText = preferText; return this; }
  /** @see #paramWriter */
  public Config paramWriter(ParamWriter paramWriter) { this.paramWriter = paramWriter; return this; }
  /** @see #poolSize */
  public Config poolSize(int poolSize) { this.poolSize = poolSize; return this; }
  /** @see #poolValidationQuery */
  public Config poolValidationQuery(String poolValidationQuery) {
    this.poolValidationQuery = poolValidationQuery;
    return this;
  }
  /** @see #connector */
  public Config connector(Function<Config, CompletableFuture<QueryReadyConnection.AutoCommit>> connector) {
    this.connector = connector;
    return this;
  }
  /** @see #ioConnector */
  public Config ioConnector(Function<Config, CompletableFuture<? extends ConnectionIo>> ioConnector) {
    this.ioConnector = ioConnector;
    return this;
  }
  /** @see #sslContextOverride */
  public Config sslContextOverride(SSLContext sslContextOverride) {
    this.sslContextOverride = sslContextOverride;
    return this;
  }
  /** @see #sslWrapper */
  public Config sslWrapper(Function<ConnectionIo, CompletableFuture<? extends ConnectionIo>> sslWrapper) {
    this.sslWrapper = sslWrapper;
    return this;
  }
}
