package asyncpg;

import java.util.concurrent.CompletableFuture;

public class QueryResultConnection<T extends QueryReadyConnection<T>> extends StartedConnection {
  protected QueryResultConnection(ConnectionContext ctx) { super(ctx); }

  // TODO: what's the best way to stream rows here?

  public CompletableFuture<T> done() {
    throw new UnsupportedOperationException();
  }
}
