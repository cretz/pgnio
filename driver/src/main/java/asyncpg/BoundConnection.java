package asyncpg;

import java.util.concurrent.CompletableFuture;

public class BoundConnection<T extends QueryReadyConnection<T>> extends StartedConnection {
  protected BoundConnection(ConnectionContext ctx) { super(ctx); }

  public CompletableFuture<QueryResultConnection<T>> execute() {
    throw new UnsupportedOperationException();
  }

  public CompletableFuture<SuspendableConnection<T>> execute(int maxRows) {
    throw new UnsupportedOperationException();
  }

  public CompletableFuture<T> done() {
    throw new UnsupportedOperationException();
  }

  public static class Reusable extends BoundConnection<QueryReadyConnection.InTransaction> {
    public final NamedBound bound;

    protected Reusable(ConnectionContext ctx, NamedBound bound) {
      super(ctx);
      this.bound = bound;
    }
  }
}
