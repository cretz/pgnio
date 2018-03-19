package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public class Config {
  public String hostname = "localhost";
  public int port = 5432;
  public String username = "postgres";
  public @Nullable String password;
  public @Nullable String database;
  // Null means try but fallback, true means required, false means never
  public @Nullable Boolean ssl = false;
  public long defaultTimeout;
  public TimeUnit defaultTimeoutUnit = TimeUnit.MILLISECONDS;
  public boolean directBuffer = true;
  public int bufferStep = 1000;
  public int protocolVersion = 196608;
  public @Nullable Map<String, String> additionalStartupParams;
  public boolean logNotices = true;
  public boolean preferText = true;
  public ParamWriter paramWriter = ParamWriter.DEFAULT;
  public int poolSize = 5;

  @SuppressWarnings("initialization")
  public Function<Config, CompletableFuture<QueryReadyConnection.AutoCommit>> connector = Connection::authed;

  @SuppressWarnings("initialization")
  public Supplier<CompletableFuture<? extends ConnectionIo>> ioConnector =
      () -> ConnectionIo.AsyncSocketChannel.connect(this);

  // Must be initialized
  // TODO: trust createSSLEngine is thread safe?
  public @Nullable SSLContext sslContextOverride;

  @SuppressWarnings("initialization")
  public Function<ConnectionIo, CompletableFuture<? extends ConnectionIo>> sslWrapper = (io) -> {
    try {
      SSLContext ctx = sslContextOverride == null ? SSLContext.getDefault() : sslContextOverride;
      ConnectionIo.Ssl sslIo = new ConnectionIo.Ssl(io, ctx.createSSLEngine(),
          directBuffer, defaultTimeout, defaultTimeoutUnit);
      return sslIo.start().thenApply(__ -> sslIo);
    } catch (NoSuchAlgorithmException e) { throw new RuntimeException(e); }
  };

  public Config hostname(String hostname) { this.hostname = hostname; return this; }
  public Config port(int port) { this.port = port; return this; }
  public Config username(String username) { this.username = username; return this; }
  public Config password(String password) { this.password = password; return this; }
  public Config database(String database) { this.database = database; return this; }
  // Null means try but fallback, true means required, false means never
  public Config ssl(@Nullable Boolean ssl) { this.ssl = ssl; return this; }
  public Config defaultTimeout(long defaultTimeout, TimeUnit defaultTimeoutUnit) {
    this.defaultTimeout = defaultTimeout;
    this.defaultTimeoutUnit = defaultTimeoutUnit;
    return this;
  }
  public Config directBuffer(boolean directBuffer) { this.directBuffer = directBuffer; return this; }
  public Config bufferStep(int bufferStep) { this.bufferStep = bufferStep; return this; }
  public Config protocolVersion(int protocolVersion) { this.protocolVersion = protocolVersion; return this; }
  public Config additionalStartupParams(Map<String, String> additionalStartupParams) {
    this.additionalStartupParams = additionalStartupParams;
    return this;
  }
  public Config logNotices(boolean logNotices) { this.logNotices = logNotices; return this; }
  public Config preferText(boolean preferText) { this.preferText = preferText; return this; }
  public Config paramWriter(ParamWriter paramWriter) { this.paramWriter = paramWriter; return this; }
  public Config poolSize(int poolSize) { this.poolSize = poolSize; return this; }
  public Config connector(Function<Config, CompletableFuture<QueryReadyConnection.AutoCommit>> connector) {
    this.connector = connector;
    return this;
  }
  public Config ioConnector(Supplier<CompletableFuture<? extends ConnectionIo>> ioConnector) {
    this.ioConnector = ioConnector;
    return this;
  }
  // Should come initialized
  public Config sslContextOverride(SSLContext sslContextOverride) {
    this.sslContextOverride = sslContextOverride;
    return this;
  }
  public Config sslWrapper(Function<ConnectionIo, CompletableFuture<? extends ConnectionIo>> sslWrapper) {
    this.sslWrapper = sslWrapper;
    return this;
  }
}
