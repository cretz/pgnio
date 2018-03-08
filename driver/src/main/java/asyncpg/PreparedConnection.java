package asyncpg;

import java.util.concurrent.CompletableFuture;

public class PreparedConnection<T extends QueryReadyConnection<T>> extends StartedConnection {
  protected PreparedConnection(ConnectionContext ctx) { super(ctx); }

  public CompletableFuture<BoundConnection<T>> bind(Object... params) {
    throw new UnsupportedOperationException();
  }

  // Only works in transaction
  public CompletableFuture<BoundConnection.Reusable> bindReusable(String name, Object... params) {
    throw new UnsupportedOperationException();
  }

  public CompletableFuture<T> done() {
    throw new UnsupportedOperationException();
  }

  public static class Reusable<T extends QueryReadyConnection<T>> extends PreparedConnection<T> {
    public final NamedPrepared prepared;

    protected Reusable(ConnectionContext ctx, NamedPrepared prepared) {
      super(ctx);
      this.prepared = prepared;
    }
  }
}
