package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

public class ConnectionPool {
  protected final Config config;
  protected final BlockingQueue<CompletableFuture<QueryReadyConnection.AutoCommit>> connections;

  @SuppressWarnings("initialization")
  public ConnectionPool(Config config) {
    this.config = config;
    connections = new LinkedBlockingQueue<>(config.poolSize);
    // We'll be somewhat eager here
    for (int i = 0; i < config.poolSize; i++) {
      try {
        connections.put(newConnection());
      } catch (InterruptedException e) { throw new RuntimeException(e); }
    }
  }

  // Must call returnConnection, even if this fails
  public CompletableFuture<QueryReadyConnection.AutoCommit> borrowConnection() {
    CompletableFuture<QueryReadyConnection.AutoCommit> fut;
    try {
      fut = connections.take();
    } catch (InterruptedException e) { throw new RuntimeException(e); }
    return fut.thenCompose(conn -> conn.ctx.io.isOpen() ? CompletableFuture.completedFuture(conn) : newConnection());
  }

  protected CompletableFuture<QueryReadyConnection.AutoCommit> newConnection() {
    return config.connector.apply(config);
  }

  // if it's null, we'll create a new one
  public void returnConnection(QueryReadyConnection.@Nullable AutoCommit conn) {
    try {
      // Only put back if still open. No, we don't deal with errors here before creating a new connection.
      // This is because it becomes too complicated, instead we just make the next take fail.
      connections.put(conn != null && conn.ctx.io.isOpen() ? conn.reset() : newConnection());
    } catch (InterruptedException e) { throw new RuntimeException(e); }
  }

  public <T> CompletableFuture<T> withConnection(Function<QueryReadyConnection.AutoCommit, CompletableFuture<T>> fn) {
    return borrowConnection().
        // Handle exception early even on borrow
        whenComplete((__, ex) -> { if (ex != null) returnConnection(null); }).
        // Otherwise release no matter what happens in fn
        thenCompose(conn -> fn.apply(conn).whenComplete((__, ___) -> returnConnection(conn)));
  }
}
