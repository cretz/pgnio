package pgnio;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple connection pool. As connections are borrowed or returned, they are evicted and replaced by new connections
 * if they are no longer open/valid. Otherwise, connections are reset via {@link Connection.Started#fullReset()} before
 * placed back into the pool. Developers are encouraged to use {@link #withConnection(Function)} and to return a future
 * which will guarantee the connection is returned to the pool even on error. If developers use
 * {@link #borrowConnection()}, they must call {@link #returnConnection(QueryReadyConnection.AutoCommit)} even on error
 * and even if it's with a null value. Not doing so will prevent the pool from maintaining its fixed size.
 */
public class ConnectionPool implements AutoCloseable {
  protected static final Logger log = Logger.getLogger(ConnectionPool.class.getName());
  protected final Config config;
  protected final BlockingQueue<CompletableFuture<QueryReadyConnection.AutoCommit>> connections;
  protected volatile boolean closed;
  /**
   * The number of connections we can still create during {@link #borrowConnection()} if there are none currently
   * available. This is set to the pool size unless we're eagerly loading at which point it's set to 0. If this value is
   * 0, borrowing will wait for next queue value.
   */
  protected final AtomicInteger remainingNewConnectionsOnBorrow = new AtomicInteger();

  /** Create a connection pool for the given connection with a fixed size of {@link Config#poolSize} */
  @SuppressWarnings("initialization")
  public ConnectionPool(Config config) {
    this.config = config;
    connections = new LinkedBlockingQueue<>(config.poolSize);
    // If we are eagerly loading, create all of the connections now
    if (config.poolConnectEagerly) {
      log.log(Level.FINE, "Creating {0} pool connections eagerly", config.poolSize);
      for (int i = 0; i < config.poolSize; i++) {
        try {
          connections.put(newConnection());
        } catch (InterruptedException e) { throw new RuntimeException(e); }
      }
    } else {
      log.log(Level.FINE, "Creating {0} pool connections lazily", config.poolSize);
      remainingNewConnectionsOnBorrow.set(config.poolSize);
    }

  }

  /** Shortcut for {@link #borrowConnection(long, TimeUnit)} using configured default timeouts */
  public CompletableFuture<QueryReadyConnection.AutoCommit> borrowConnection() {
    return borrowConnection(config.defaultTimeout, config.defaultTimeoutUnit);
  }

  /**
   * Borrow a connection for use. If a connection is not available, this blocks until one is made available or the
   * timeout is reached (which throws an {@link IllegalStateException}). A timeout of zero means wait indefinitely. This
   * may internally create a new connection if the one that would be returned is no longer open/valid or if the
   * connections were not eagerly created and the pool size has not been reached. Developers who call this directly must
   * make sure to call {@link #returnConnection(QueryReadyConnection.AutoCommit)} when they are through with the
   * connection (or pass null if they don't want the connection in the pool anymore) even on failure. For simplicity,
   * developers are encouraged to instead use {@link #withConnection(Function)} when they can.
   */
  public CompletableFuture<QueryReadyConnection.AutoCommit> borrowConnection(long timeout, TimeUnit unit) {
    if (closed) throw new IllegalStateException("Pool is closed");
    CompletableFuture<QueryReadyConnection.AutoCommit> fut = null;
    try {
      // If there are some remaining for first creation, we can create anew instead of waiting
      if (remainingNewConnectionsOnBorrow.get() > 0) {
        fut = connections.poll();
        if (fut == null) {
          int preDecrementValue = remainingNewConnectionsOnBorrow.getAndDecrement();
          if (preDecrementValue > 0) {
            // Non-blocking get, then create since we have room
            if (log.isLoggable(Level.FINE))
              log.log(Level.FINE,
                  "No connections available; lazily creating one, {0} connections remaining to be lazily created",
                  preDecrementValue - 1);
            fut = newConnection();
          } else {
            remainingNewConnectionsOnBorrow.set(0);
          }
        }
      }
      // If the above didn't happen, we have to block on reading from the queue
      if (fut == null) {
        fut = timeout == 0L ? connections.take() : connections.poll(timeout, unit);
        if (fut == null) throw new IllegalStateException("Timeout waiting for connection");
      }
    } catch (InterruptedException e) { throw new RuntimeException(e); }
    // Throw if not open so a new one is made
    fut = fut.thenApply(conn -> {
      if (!conn.ctx.io.isOpen()) throw new IllegalStateException("Not open");
      return conn;
    });
    // If there is a validation query, run it and terminate on failure
    if (config.poolValidationQuery != null) {
      String validationQuery = config.poolValidationQuery;
      fut = fut.thenCompose(c ->
          c.simpleQueryExec(validationQuery).handle((conn, ex) -> {
            if (ex != null)
              return c.terminate().handle((__, ___) -> {
                if (ex instanceof RuntimeException) throw (RuntimeException) ex;
                throw new RuntimeException(ex);
              }).thenApply(__ -> c);
            return CompletableFuture.completedFuture(conn);
          }).thenCompose(Function.identity()));
    }
    // On any error, create new connection...otherwise return existing
    return fut.handle((conn, err) -> {
      if (err == null) return CompletableFuture.completedFuture(conn);
      log.log(Level.WARNING, "Error on borrowed connection, returning new one", err);
      return newConnection();
    }).thenCompose(Function.identity());
  }

  protected CompletableFuture<QueryReadyConnection.AutoCommit> newConnection() {
    return config.connector.apply(config);
  }

  /**
   * Return a connection to the pool. If the given connection is null or not open, a new connection is added to the pool
   * instead. This should never block so long as {@link #borrowConnection()} was previously called. For simplicity,
   * developers are encouraged to use {@link #withConnection(Function)} instead. If a connection is returned on a closed
   * pool, by default it is closed first (ref {@link Config#poolCloseReturnedConnectionOnClosedPool}), then an
   * {@link IllegalStateException} is thrown.
   */
  @SuppressWarnings("dereference.of.nullable")
  public void returnConnection(QueryReadyConnection.@Nullable AutoCommit conn) {
    if (closed) {
      if (conn != null && conn.ctx.io.isOpen() && config.poolCloseReturnedConnectionOnClosedPool) {
        log.log(Level.FINE, "Connection returned to closed pool, attempting to close");
        try {
          conn.close();
        } catch (Exception e) {
          throw new IllegalStateException("Pool is closed, also failed closing returned connection", e);
        }
      }
      throw new IllegalStateException("Pool is closed");
    }
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

  /** Shortcut to {@link #withConnection(long, TimeUnit, Function)} using the configured default timeout */
  public <T> CompletableFuture<T> withConnection(Function<QueryReadyConnection.AutoCommit, CompletableFuture<T>> fn) {
    return withConnection(config.defaultTimeout, config.defaultTimeoutUnit, fn);
  }

  /**
   * Invoke the function with a borrowed connection or throw if timeout reached waiting for one. A timeout of 0 waits
   * forever for a connection. The result of the function must be a future and the connection is guaranteed to be
   * returned to the pool on completion of the resulting future. The result of this method is the future from the
   * callback (with the return-to-pool step added). This is a simpler alternative to {@link #borrowConnection()} +
   * {@link #returnConnection(QueryReadyConnection.AutoCommit)} which does things like make sure the connection is
   * returned even on error. Note, if an error happens directly within the callback instead of returned as an errored
   * future, it will be wrapped and turned into an errored future.
   */
  public <T> CompletableFuture<T> withConnection(long waitForConnTimeout, TimeUnit waitForConnUnit,
      Function<QueryReadyConnection.AutoCommit, CompletableFuture<T>> fn) {
    return borrowConnection(waitForConnTimeout, waitForConnUnit).
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
