package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

public abstract class QueryReadyConnection<SELF extends QueryReadyConnection<SELF>> extends Connection.Started {
  protected QueryReadyConnection(Context ctx) { super(ctx); }

  public @Nullable TransactionStatus getTransactionStatus() { return ctx.lastTransactionStatus; }

  protected CompletableFuture<Void> sendQuery(String query) {
    ctx.buf.clear();
    ctx.writeByte((byte) 'Q').writeLengthIntBegin().writeCString(query).writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  @SuppressWarnings("unchecked")
  public CompletableFuture<QueryResultConnection<SELF>> simpleQuery(String query) {
    assertValid();
    invalid = true;
    log.log(Level.FINE, "Running simple query: {0}", query);
    return sendQuery(query).thenApply(__ -> new QueryResultConnection<>(ctx, (SELF) this, true));
  }

  public CompletableFuture<List<QueryMessage.Row>> simpleQueryRows(String query) {
    return simpleQuery(query).thenCompose(QueryResultConnection::collectRowsAndDone);
  }

  @SuppressWarnings({"return.type.incompatible", "methodref.return.invalid"})
  public CompletableFuture<@Nullable Long> simpleQueryRowCount(String query) {
    return simpleQuery(query).thenCompose(QueryResultConnection::collectRowCountAndDone);
  }

  public CompletableFuture<SELF> simpleQueryExec(String query) {
    return simpleQuery(query).thenCompose(QueryResultConnection::done);
  }

  protected CompletableFuture<Void> sendParse(String statementName, String query, int... parameterDataTypes) {
    ctx.buf.clear();
    ctx.writeByte((byte) 'P').writeLengthIntBegin().writeCString(statementName).writeCString(query).
        writeShort((short) parameterDataTypes.length);
    for (int parameterDataType : parameterDataTypes) ctx.writeInt(parameterDataType);
    ctx.writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  // Data types are not required here, just allowed
  @SuppressWarnings("unchecked")
  public CompletableFuture<QueryBuildConnection.Prepared<SELF>> prepare(String query, int... parameterDataTypes) {
    return prepareReusable("", query, parameterDataTypes);
  }

  @SuppressWarnings("unchecked")
  public CompletableFuture<QueryBuildConnection.Prepared<SELF>> prepareReusable(String statementName,
      String query, int... parameterDataTypes) {
    assertValid();
    log.log(Level.FINE, "Preparing query: {0}", query);
    return sendParse(statementName, query, parameterDataTypes).
        thenApply(__ -> new QueryBuildConnection.Prepared<>(ctx, (SELF) this, statementName));
  }

  public CompletableFuture<QueryResultConnection<SELF>> preparedQuery(String query, Object... params) {
    return prepare(query).
        thenCompose(QueryBuildConnection.Prepared::describeStatement).
        thenCompose(prepared -> prepared.bind(params)).
        thenCompose(QueryBuildConnection.Bound::execute).
        thenCompose(QueryBuildConnection::done);
  }

  // TODO: preparedReusableQuery, reusePrepare, reusePrepared, reusePreparedQuery

  public CompletableFuture<List<QueryMessage.Row>> preparedQueryRows(String query, Object... params) {
    return preparedQuery(query, params).thenCompose(QueryResultConnection::collectRowsAndDone);
  }

  @SuppressWarnings({"return.type.incompatible", "methodref.return.invalid"})
  public CompletableFuture<@Nullable Long> preparedQueryRowCount(String query, Object... params) {
    return preparedQuery(query, params).thenCompose(QueryResultConnection::collectRowCountAndDone);
  }

  public CompletableFuture<SELF> preparedQueryExec(String query, Object... params) {
    return preparedQuery(query, params).thenCompose(QueryResultConnection::done);
  }

  public enum TransactionStatus { IDLE, IN_TRANSACTION, FAILED_TRANSACTION }

  public static class AutoCommit extends QueryReadyConnection<AutoCommit> {
    protected AutoCommit(Context ctx) { super(ctx); }

    public CompletableFuture<InTransaction> beginTransaction() {
      assertValid();
      throw new UnsupportedOperationException();
    }
  }

  public static class InTransaction extends QueryReadyConnection<InTransaction> {
    protected InTransaction(Context ctx) { super(ctx); }

    public CompletableFuture<AutoCommit> commitTransaction() {
      assertValid();
      throw new UnsupportedOperationException();
    }

    public CompletableFuture<AutoCommit> rollbackTransaction() {
      assertValid();
      throw new UnsupportedOperationException();
    }

    // TODO: reuseBound
//    public CompletableFuture<BoundConnection.Reusable> reuseBound(NamedBound bound) {
//      assertValid();
//      throw new UnsupportedOperationException();
//    }
  }
}
