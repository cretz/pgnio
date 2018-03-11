package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.stream.Collectors;

public abstract class QueryReadyConnection<SELF extends QueryReadyConnection<SELF>> extends Connection.Started {
  protected QueryReadyConnection(ConnectionContext ctx) { super(ctx); }

  public @Nullable TransactionStatus getTransactionStatus() { return ctx.lastTransactionStatus; }

  protected CompletableFuture<Void> sendQuery(String query) {
    ctx.buf.clear();
    ctx.writeByte((byte) 'Q').writeLengthIntBegin().writeString(query).writeLengthIntEnd();
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
    return simpleQuery(query).thenCompose(res ->
        res.collectRows(Collectors.toList()).thenCompose(rows ->
            res.done().thenApply(__ -> rows)));
  }

  @SuppressWarnings("return.type.incompatible") // TODO: https://github.com/typetools/checker-framework/issues/1882
  public CompletableFuture<@Nullable Long> simpleQueryRowCount(String query) {
    return simpleQuery(query).thenCompose(res ->
        res.next(QueryMessage.Complete.class).thenCompose(complete ->
            res.done().thenApply(__ -> complete == null ? null : complete.getRowCount())));
  }

  public CompletableFuture<SELF> simpleQueryExec(String query) {
    return simpleQuery(query).thenCompose(QueryResultConnection::done);
  }

  protected CompletableFuture<Void> sendParse(String statementName, String query, int... parameterDataTypes) {
    ctx.buf.clear();
    ctx.writeByte((byte) 'P').writeLengthIntBegin().writeString(statementName).writeString(query).
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
    return sendParse(statementName, query, parameterDataTypes).
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
