package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

/** Base connection state when the server is ready to execute queries */
public abstract class QueryReadyConnection<SELF extends QueryReadyConnection<SELF>> extends Connection.Started {
  protected QueryReadyConnection(Context ctx) { super(ctx); }

  /** The last sent transaction status after the last set of queries completed, or null */
  public @Nullable TransactionStatus getTransactionStatus() { return ctx.lastTransactionStatus; }

  protected CompletableFuture<Void> sendQuery(String query) {
    ctx.buf.clear();
    ctx.writeByte((byte) 'Q').writeLengthIntBegin().writeCString(query).writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  /**
   * Execute a simple query and get the results. This is roughly equivalent to {@link #prepare(String, int...)} +
   * {@link QueryBuildConnection.Prepared#bind(Object...)} + {@link QueryBuildConnection.Bound#describe()} *
   * {@link QueryBuildConnection.Bound#execute()} + {@link QueryBuildConnection.Bound#done()} but in one round trip.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<QueryResultConnection<SELF>> simpleQuery(String query) {
    assertValid();
    log.log(Level.FINE, "Running simple query: {0}", query);
    return sendQuery(query).thenApply(__ -> passControlTo(new QueryResultConnection<>(ctx, (SELF) this, true)));
  }

  /** {@link #simpleQuery(String)} + {@link QueryResultConnection#collectRowsAndDone()} */
  public CompletableFuture<List<QueryMessage.Row>> simpleQueryRows(String query) {
    return simpleQuery(query).thenCompose(QueryResultConnection::collectRowsAndDone);
  }

  /** {@link #simpleQuery(String)} + {@link QueryResultConnection#collectRowCountAndDone()} */
  @SuppressWarnings({"return.type.incompatible", "methodref.return.invalid"})
  public CompletableFuture<@Nullable Long> simpleQueryRowCount(String query) {
    return simpleQuery(query).thenCompose(QueryResultConnection::collectRowCountAndDone);
  }

  /** {@link #simpleQuery(String)} + {@link QueryResultConnection#done()} */
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

  /**
   * Parse the given query with parameters in preparation for binding and executing. While parameterDataTypes may be
   * passed, they are not required and if not present, the server will determine them from the query.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<QueryBuildConnection.Prepared<SELF>> prepare(String query, int... parameterDataTypes) {
    return prepareReusable("", query, parameterDataTypes);
  }

  /**
   * Equivalent to {@link #prepare(String, int...)} but is given a name that can be reused during this same connection
   * with {@link #reusePrepared(String)}.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<QueryBuildConnection.Prepared<SELF>> prepareReusable(String statementName,
      String query, int... parameterDataTypes) {
    assertValid();
    log.log(Level.FINE, "Preparing query: {0}", query);
    return sendParse(statementName, query, parameterDataTypes).
        thenApply(__ -> passControlTo(new QueryBuildConnection.Prepared<>(ctx, (SELF) this, statementName)));
  }

  /** Reuse a prepared query created with {@link #prepareReusable(String, String, int...)} */
  @SuppressWarnings("unchecked")
  public CompletableFuture<QueryBuildConnection.Prepared<SELF>> reusePrepared(String statementName) {
    assertValid();
    return CompletableFuture.completedFuture(
        passControlTo(new QueryBuildConnection.Prepared<>(ctx, (SELF) this, statementName)));
  }

  /** {@link #prepare(String, int...)} + {@link QueryBuildConnection.Prepared#bindDescribeExecuteAndDone(Object...)} */
  public CompletableFuture<QueryResultConnection<SELF>> preparedQuery(String query, Object... params) {
    return prepare(query).thenCompose(p -> p.bindDescribeExecuteAndDone(params));
  }

  /** {@link #preparedQuery(String, Object...)} + {@link QueryResultConnection#collectRowsAndDone()} */
  public CompletableFuture<List<QueryMessage.Row>> preparedQueryRows(String query, Object... params) {
    return preparedQuery(query, params).thenCompose(QueryResultConnection::collectRowsAndDone);
  }

  /** {@link #preparedQuery(String, Object...)} + {@link QueryResultConnection#collectRowCountAndDone()} */
  @SuppressWarnings({"return.type.incompatible", "methodref.return.invalid"})
  public CompletableFuture<@Nullable Long> preparedQueryRowCount(String query, Object... params) {
    return preparedQuery(query, params).thenCompose(QueryResultConnection::collectRowCountAndDone);
  }

  /** {@link #preparedQuery(String, Object...)} + {@link QueryResultConnection#done()} */
  public CompletableFuture<SELF> preparedQueryExec(String query, Object... params) {
    return preparedQuery(query, params).thenCompose(QueryResultConnection::done);
  }

  /** Begin a "COPY FROM STDIN" query. {@link #simpleQuery(String)} + {@link QueryResultConnection#copyIn()} */
  public CompletableFuture<QueryResultConnection.Copy<SELF>> simpleCopyIn(String query) {
    return simpleQuery(query).thenCompose(QueryResultConnection::copyIn);
  }

  /** Begin a "COPY TO STDOUT" query. {@link #simpleQuery(String)} + {@link QueryResultConnection#copyOut()} */
  public CompletableFuture<QueryResultConnection.Copy<SELF>> simpleCopyOut(String query) {
    return simpleQuery(query).thenCompose(QueryResultConnection::copyOut);
  }

  /** Known transaction statuses when a query set ends */
  public enum TransactionStatus { IDLE, IN_TRANSACTION, FAILED_TRANSACTION }

  /** Begin a transaction */
  public abstract CompletableFuture<InTransaction<SELF>> beginTransaction();

  /** Connection state that auto-commits each statement */
  public static class AutoCommit extends QueryReadyConnection<AutoCommit> {
    protected AutoCommit(Context ctx) { super(ctx); }

    @Override
    public CompletableFuture<InTransaction<AutoCommit>> beginTransaction() {
      assertValid();
      return simpleQueryExec("BEGIN").thenApply(conn -> passControlTo(new InTransaction<>(ctx, 0, conn)));
    }

    @Override
    protected CompletableFuture<AutoCommit> reset() {
      assertValid();
      log.log(Level.FINER, "Resetting from auto commit");
      return CompletableFuture.completedFuture(this);
    }
  }

  /**
   * Connection state in a transaction that won't complete until either {@link #commitTransaction()} or
   * {@link #rollbackTransaction()} called.
   */
  public static class InTransaction<T extends QueryReadyConnection<T>> extends QueryReadyConnection<InTransaction<T>> {
    protected final int depth;
    protected final T prevConn;

    protected InTransaction(Context ctx, int depth, T prevConn) {
      super(ctx);
      this.depth = depth;
      this.prevConn = prevConn;
    }

    /** Begin a nested transaction using savepoints */
    @Override
    public CompletableFuture<InTransaction<InTransaction<T>>> beginTransaction() {
      assertValid();
      return simpleQueryExec("SAVEPOINT asyncpg_sp_" + (depth + 1)).thenApply(conn ->
          passControlTo(new InTransaction<>(ctx, depth + 1, conn)));
    }

    /** Commit this transaction and return to previous state. If nested, this just releases the savepoint. */
    public CompletableFuture<T> commitTransaction() {
      assertValid();
      String query = depth == 0 ? "COMMIT" : "RELEASE SAVEPOINT asyncpg_sp_" + depth;
      return simpleQueryExec(query).thenApply(conn -> {
        conn.prevConn.resumeControl();
        return conn.prevConn;
      });
    }

    /** Rollback this transaction and return to previous state. If nested, this just rolls back to the savepoint. */
    public CompletableFuture<T> rollbackTransaction() {
      assertValid();
      String query = depth == 0 ? "ROLLBACK" : "ROLLBACK TO SAVEPOINT asyncpg_sp_" + depth;
      return simpleQueryExec(query).thenApply(conn -> {
        conn.prevConn.resumeControl();
        return conn.prevConn;
      });
    }

    /** Reuse a named bound portal created via {@link QueryBuildConnection.Prepared#bindReusable(String, Object...)} */
    public CompletableFuture<QueryBuildConnection.Bound<InTransaction<T>>> reuseBound(String portalName) {
      return CompletableFuture.completedFuture(new QueryBuildConnection.Bound<>(ctx, this, null, portalName));
    }

    @Override
    protected CompletableFuture<AutoCommit> reset() {
      assertValid();
      log.log(Level.FINER, "Resetting from in transaction");
      return rollbackTransaction().thenCompose(Started::reset);
    }
  }
}
