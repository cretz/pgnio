package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple connection pool. As connections are borrowed or returned, they are evicted and replaced by new connections
 * if they are no longer open. Otherwise, connections are reset via {@link Connection.Started#fullReset()} before placed
 * back into the pool. Developers are encouraged to use {@link #withConnection(Function)} and to return a future which
 * will guarantee the connection is returned to the pool even on error. If developers use {@link #borrowConnection()},
 * they must call {@link #returnConnection(QueryReadyConnection.AutoCommit)} even on error and even if it's will a null
 * value. Not doing so will prevent the pool from maintaining its fixed size.
 */
public class ConnectionPool implements AutoCloseable {
  protected static final Logger log = Logger.getLogger(ConnectionPool.class.getName());
  protected final Config config;
  protected final BlockingQueue<CompletableFuture<QueryReadyConnection.AutoCommit>> connections;
  protected volatile boolean closed;

  /** Create a connection pool for the given connection with a fixed size of {@link Config#poolSize} */
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

  /**
   * Borrow a connection for use. If a connection is not available, this blocks until one is made available. This may
   * internally create a new connection if the one that would be returned is no longer open. Developers who call this
   * directly must make sure to call {@link #returnConnection(QueryReadyConnection.AutoCommit)} when they are through
   * with the connection (or pass null if they don't want the connection in the pool anymore) even on failure. For
   * simplicity, developers are encouraged to instead use {@link #withConnection(Function)} when they can.
   */
  public CompletableFuture<QueryReadyConnection.AutoCommit> borrowConnection() {
    if (closed) throw new IllegalStateException("Pool is closed");
    CompletableFuture<QueryReadyConnection.AutoCommit> fut;
    try {
      fut = connections.take();
    } catch (InterruptedException e) { throw new RuntimeException(e); }
    return fut.thenCompose(conn -> conn.ctx.io.isOpen() ? CompletableFuture.completedFuture(conn) : newConnection());
  }

  protected CompletableFuture<QueryReadyConnection.AutoCommit> newConnection() {
    return config.connector.apply(config);
  }

  /**
   * Return a connection to the pool. If the given connection is null or not open, a new connection is added to the pool
   * instead. This should never block so long as {@link #borrowConnection()} was previously called. For simplicity,
   * developers are encouraged to use {@link #withConnection(Function)} instead.
   */
  @SuppressWarnings("dereference.of.nullable")
  public void returnConnection(QueryReadyConnection.@Nullable AutoCommit conn) {
    if (closed) throw new IllegalStateException("Pool is closed");
    try {
      // Only put back if still open. No, we don't deal with errors here before creating a new connection.
      // This is because it becomes too complicated, instead we just make the next take fail.
      if (conn == null || !conn.ctx.io.isOpen()) {
        connections.put(newConnection());
      } else {
        // We have to remove all subscriptions
        connections.put(conn.fullReset().whenComplete((__, ___) -> {
          conn.notices().unsubscribeAll();
          conn.notifications().unsubscribeAll();
          conn.parameterStatuses().unsubscribeAll();
        }));
      }
    } catch (InterruptedException e) { throw new RuntimeException(e); }
  }

  /**
   * Invoke the function with a borrowed connection. The result of the function must be a future and the connection is
   * guaranteed to be returned to the pool on completion of the resulting future. The result of this method is the
   * future from the callback (with the return-to-pool step added). This is a simpler alternative to
   * {@link #borrowConnection()} + {@link #returnConnection(QueryReadyConnection.AutoCommit)} which does things like
   * make sure the connection is returned even on error. Note, if an error happens directly within the callback instead
   * of returned as an errored future, it will be wrapped and turned into an errored future.
   */
  public <T> CompletableFuture<T> withConnection(Function<QueryReadyConnection.AutoCommit, CompletableFuture<T>> fn) {
    return borrowConnection().
        // Handle exception early even on borrow
        whenComplete((__, ex) -> { if (ex != null) returnConnection(null); }).
        // Otherwise release no matter what happens in fn
        thenCompose(conn -> {
          // Make the call, wrapping even direct exceptions into the future
          CompletableFuture<T> fut;
          try {
            fut = fn.apply(conn);
          } catch (Throwable e) {
            fut = new CompletableFuture<>();
            fut.completeExceptionally(e);
          }
          return fut.whenComplete((__, ex) -> {
            if (ex != null) log.log(Level.WARNING, "Ignoring error when returning connection to pool", ex);
            returnConnection(conn);
          });
        });
  }

  /** Empty the pool, mark it closed, and terminate all connections being held */
  public CompletableFuture<Void> terminateAll() {
    closed = true;
    List<CompletableFuture<QueryReadyConnection.AutoCommit>> futs = new ArrayList<>();
    connections.drainTo(futs);
    CompletableFuture[] closedFuts = new CompletableFuture[futs.size()];
    for (int i = 0; i < closedFuts.length; i++) closedFuts[i] = futs.get(i).thenCompose(Connection::terminate);
    return CompletableFuture.allOf(closedFuts);
  }

  /** Call {@link #terminateAll()} and wait for completion */
  @Override
  public void close() throws ExecutionException, InterruptedException { terminateAll().get(); }
}
