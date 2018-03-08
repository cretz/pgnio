package asyncpg;

import asyncpg.nio.AsynchronousSocketChannelConnectionIo;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class Config {
  public String hostname = "localhost";
  public int port = 5432;
  public String username;
  public String password;
  public String database;
  public boolean ssl;
  public long defaultTimeout;
  public TimeUnit defaultTimeoutUnit = TimeUnit.MILLISECONDS;
  public boolean directBuffer = true;
  public int bufferStep = 1000;
  public int protocolVersion = 196608;
  public Map<String, String> additionalStartupParams;
  public boolean logNotices = true;

  public Supplier<CompletableFuture<? extends ConnectionIo>> connector =
      () -> AsynchronousSocketChannelConnectionIo.connect(this);

  public Config hostname(String hostname) { this.hostname = hostname; return this; }
  public Config port(int port) { this.port = port; return this; }
  public Config username(String username) { this.username = username; return this; }
  public Config password(String password) { this.password = password; return this; }
  public Config database(String database) { this.database = database; return this; }
  public Config ssl(boolean ssl) { this.ssl = ssl; return this; }
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
}
