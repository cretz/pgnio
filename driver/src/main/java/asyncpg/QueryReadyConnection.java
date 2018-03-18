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
    invalid = true;
    log.log(Level.FINE, "Preparing query: {0}", query);
    return sendParse(statementName, query, parameterDataTypes).
        thenApply(__ -> new QueryBuildConnection.Prepared<>(ctx, (SELF) this, statementName));
  }

  @SuppressWarnings("unchecked")
  public CompletableFuture<QueryBuildConnection.Prepared<SELF>> reusePrepared(String statementName) {
    assertValid();
    invalid = true;
    return CompletableFuture.completedFuture(new QueryBuildConnection.Prepared<>(ctx, (SELF) this, statementName));
  }

  public CompletableFuture<QueryResultConnection<SELF>> preparedQuery(String query, Object... params) {
    return prepare(query).thenCompose(p -> p.describeBindExecuteAndDone(params));
  }

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

  public CompletableFuture<QueryResultConnection.Copy<SELF>> simpleCopyIn(String query) {
    return simpleQuery(query).thenCompose(QueryResultConnection::copyIn);
  }

  public CompletableFuture<QueryResultConnection.Copy<SELF>> simpleCopyOut(String query) {
    return simpleQuery(query).thenCompose(QueryResultConnection::copyOut);
  }

  public enum TransactionStatus { IDLE, IN_TRANSACTION, FAILED_TRANSACTION }

  public abstract CompletableFuture<InTransaction<SELF>> beginTransaction();

  public static class AutoCommit extends QueryReadyConnection<AutoCommit> {
    protected AutoCommit(Context ctx) { super(ctx); }

    @Override
    public CompletableFuture<InTransaction<AutoCommit>> beginTransaction() {
      assertValid();
      return simpleQueryExec("BEGIN").thenApply(conn -> {
        conn.invalid = true;
        return new InTransaction<>(ctx, 0, conn);
      });
    }
  }

  public static class InTransaction<T extends QueryReadyConnection<T>> extends QueryReadyConnection<InTransaction<T>> {
    protected final int depth;
    protected final T prevConn;

    protected InTransaction(Context ctx, int depth, T prevConn) {
      super(ctx);
      this.depth = depth;
      this.prevConn = prevConn;
    }

    @Override
    public CompletableFuture<InTransaction<InTransaction<T>>> beginTransaction() {
      assertValid();
      return simpleQueryExec("SAVEPOINT asyncpg_sp_" + (depth + 1)).thenApply(conn -> {
        conn.invalid = true;
        return new InTransaction<>(ctx, depth + 1, conn);
      });
    }

    public CompletableFuture<T> commitTransaction() {
      assertValid();
      String query = depth == 0 ? "COMMIT" : "RELEASE SAVEPOINT asyncpg_sp_" + depth;
      return simpleQueryExec(query).thenApply(conn -> {
        conn.prevConn.invalid = false;
        return conn.prevConn;
      });
    }

    public CompletableFuture<T> rollbackTransaction() {
      assertValid();
      String query = depth == 0 ? "ROLLBACK" : "ROLLBACK TO SAVEPOINT asyncpg_sp_" + depth;
      return simpleQueryExec(query).thenApply(conn -> {
        conn.prevConn.invalid = false;
        return conn.prevConn;
      });
    }
  }
}
