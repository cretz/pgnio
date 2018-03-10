package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.CompletableFuture;

public abstract class QueryReadyConnection<SELF extends QueryReadyConnection<SELF>> extends StartedConnection {
  protected QueryReadyConnection(ConnectionContext ctx) { super(ctx); }

  public @Nullable TransactionStatus getTransactionStatus() { return ctx.lastTransactionStatus; }

  protected CompletableFuture<Void> sendQuery(String sql) {
    ctx.buf.clear();
    ctx.writeByte((byte) 'Q').writeLengthIntBegin().writeString(sql).writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  @SuppressWarnings("unchecked")
  public CompletableFuture<QueryResultConnection<SELF>> simpleQuery(String sql) {
    assertValid();
    invalid = true;
    return sendQuery(sql).thenApply(__ -> new QueryResultConnection<>(ctx, (SELF) this, true));
  }

  protected CompletableFuture<Void> sendParse(String statementName, String sql, int... parameterDataTypes) {
    ctx.buf.clear();
    ctx.writeByte((byte) 'P').writeLengthIntBegin().writeString(statementName).writeString(sql).
        writeShort((short) parameterDataTypes.length);
    for (int parameterDataType : parameterDataTypes) ctx.writeInt(parameterDataType);
    ctx.writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  // Data types are not required here, just allowed
  @SuppressWarnings("unchecked")
  public CompletableFuture<QueryBuildConnection.Prepared<SELF>> prepare(String sql, int... parameterDataTypes) {
    return prepareReusable("", sql, parameterDataTypes);
  }

  @SuppressWarnings("unchecked")
  public CompletableFuture<QueryBuildConnection.Prepared<SELF>> prepareReusable(String statementName,
      String sql, int... parameterDataTypes) {
    assertValid();
    return sendParse(statementName, sql, parameterDataTypes).
        thenApply(__ -> new QueryBuildConnection.Prepared<>(ctx, (SELF) this, statementName));
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

//    public CompletableFuture<BoundConnection.Reusable> reuseBound(NamedBound bound) {
//      assertValid();
//      throw new UnsupportedOperationException();
//    }
  }
}
