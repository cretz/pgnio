package asyncpg;

import java.util.concurrent.CompletableFuture;

public abstract class QueryReadyConnection<T extends QueryReadyConnection<T>> extends StartedConnection {
  protected QueryReadyConnection(ConnectionContext ctx) { super(ctx); }

  public CompletableFuture<QueryResultConnection<T>> simpleQuery(String sql) {
    throw new UnsupportedOperationException();
  }

  public CompletableFuture<PreparedConnection<T>> prepare(String sql, DataType... parameterDataTypes) {
    throw new UnsupportedOperationException();
  }

  public CompletableFuture<PreparedConnection.Reusable<T>> prepareReusable(
      String name, String sql, DataType... parameterDataTypes) {
    throw new UnsupportedOperationException();
  }

  public CompletableFuture<PreparedConnection.Reusable<T>> reusePrepared(NamedPrepared prepared) {
    throw new UnsupportedOperationException();
  }

  public static class AutoCommit extends QueryReadyConnection<AutoCommit> {
    protected AutoCommit(ConnectionContext ctx) { super(ctx); }

    public CompletableFuture<InTransaction> beginTransaction() {
      throw new UnsupportedOperationException();
    }
  }

  public static class InTransaction extends QueryReadyConnection<InTransaction> {
    protected InTransaction(ConnectionContext ctx) { super(ctx); }

    public CompletableFuture<AutoCommit> commitTransaction() {
      throw new UnsupportedOperationException();
    }

    public CompletableFuture<AutoCommit> rollbackTransaction() {
      throw new UnsupportedOperationException();
    }

    public CompletableFuture<BoundConnection.Reusable> reuseBound(NamedBound bound) {
      throw new UnsupportedOperationException();
    }
  }
}
