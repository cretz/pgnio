package asyncpg;

import java.util.concurrent.CompletableFuture;

public abstract class QueryReadyConnection<T extends QueryReadyConnection<T>> extends StartedConnection {
  // True when inside a query or something similar
  protected boolean invalid;

  protected QueryReadyConnection(ConnectionContext ctx) { super(ctx); }

  public TransactionStatus getTransactionStatus() { return ctx.lastTransactionStatus; }

  protected void assertValid() {
    if (invalid) throw new IllegalStateException("Not ready for queries");
  }

  public CompletableFuture<QueryResultConnection<T>> simpleQuery(String sql) {
    assertValid();
    invalid = true;
    // Send Query message
    ctx.buf.clear();
    ctx.bufWriteByte((byte) 'Q').bufLengthIntBegin().bufWriteString(sql).bufLengthIntEnd();
    ctx.buf.flip();
    @SuppressWarnings("unchecked")
    CompletableFuture<QueryResultConnection<T>> ret =
        writeFrontendMessage().thenApply(__ -> new QueryResultConnection<>(ctx, (T) this));
    return ret;
  }

  public CompletableFuture<PreparedConnection<T>> prepare(String sql, DataType... parameterDataTypes) {
    assertValid();
    throw new UnsupportedOperationException();
  }

  public CompletableFuture<PreparedConnection.Reusable<T>> prepareReusable(
      String name, String sql, DataType... parameterDataTypes) {
    assertValid();
    throw new UnsupportedOperationException();
  }

  public CompletableFuture<PreparedConnection.Reusable<T>> reusePrepared(NamedPrepared prepared) {
    assertValid();
    throw new UnsupportedOperationException();
  }

  public enum TransactionStatus { IDLE, IN_TRANSACTION, FAILED_TRANSACTION }

  public static class AutoCommit extends QueryReadyConnection<AutoCommit> {
    protected AutoCommit(ConnectionContext ctx) { super(ctx); }

    public CompletableFuture<InTransaction> beginTransaction() {
      assertValid();
      throw new UnsupportedOperationException();
    }
  }

  public static class InTransaction extends QueryReadyConnection<InTransaction> {
    protected InTransaction(ConnectionContext ctx) { super(ctx); }

    public CompletableFuture<AutoCommit> commitTransaction() {
      assertValid();
      throw new UnsupportedOperationException();
    }

    public CompletableFuture<AutoCommit> rollbackTransaction() {
      assertValid();
      throw new UnsupportedOperationException();
    }

    public CompletableFuture<BoundConnection.Reusable> reuseBound(NamedBound bound) {
      assertValid();
      throw new UnsupportedOperationException();
    }
  }
}
