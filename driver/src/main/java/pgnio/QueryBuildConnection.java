package pgnio;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

/** Base connection state for advanced query building using prepared and bound statements */
public abstract class QueryBuildConnection
    <T extends QueryReadyConnection<T>, SELF extends QueryBuildConnection<T, SELF>> extends Connection.Started {
  protected final T prevConn;

  protected QueryBuildConnection(Context ctx, T prevConn) {
    super(ctx);
    this.prevConn = prevConn;
  }

  protected CompletableFuture<Void> sendFlush() {
    ctx.buf.clear();
    ctx.writeByte((byte) 'H').writeLengthIntBegin().writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  /**
   * Send a flush for reading results. This can be called instead of {@link #done()} to read responses before ending
   * the advanced query building process. {@link QueryResultConnection#done()} will then return here. Note, the query
   * result will not end with a fixed "done", so continually calling {@link QueryResultConnection#next()} may hang
   * instead of return null.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<QueryResultConnection<SELF>> flush() {
    return sendFlush().thenApply(__ -> new QueryResultConnection<>(ctx, (SELF) this, false));
  }

  protected CompletableFuture<Void> sendClose(boolean portal, String name) {
    ctx.buf.clear();
    ctx.writeByte((byte) 'C').writeLengthIntBegin().writeByte((byte) (portal ? 'P' : 'S')).
        writeCString(name).writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  protected CompletableFuture<Void> sendDescribe(boolean portal, String name) {
    ctx.buf.clear();
    ctx.writeByte((byte) 'D').writeLengthIntBegin().writeByte((byte) (portal ? 'P' : 'S')).
        writeCString(name).writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  /** Describe either the statement or bound portal */
  public abstract CompletableFuture<SELF> describe();

  protected CompletableFuture<Void> sendSync() {
    ctx.buf.clear();
    ctx.writeByte((byte) 'S').writeLengthIntBegin().writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  @Override
  protected CompletableFuture<QueryReadyConnection.AutoCommit> reset() {
    assertValid();
    log.log(Level.FINER, "Resetting from query build");
    return done().thenCompose(QueryResultConnection::reset);
  }

  /** Complete the sending of advanced query building commands and tell Postgres to start sending responses */
  public CompletableFuture<QueryResultConnection<T>> done() {
    return sendSync().thenApply(__ -> {
      // We have to resume control and then re-pass it here
      prevConn.resumeControl();
      return prevConn.passControlTo(new QueryResultConnection<>(ctx, prevConn, true));
    });
  }

  /** Connection state when statements have been parsed and can be described, bound, etc */
  public static class Prepared<T extends QueryReadyConnection<T>> extends QueryBuildConnection<T, Prepared<T>> {
    /** Format that can be used in {@link #bindEx(boolean[], boolean[], Object...)} for all text format */
    public static final boolean[] FORMAT_TEXT_ALL = new boolean[0];
    /** Format that can be used in {@link #bindEx(boolean[], boolean[], Object...)} for all binary format */
    public static final boolean[] FORMAT_BINARY_ALL = new boolean[] { false };
    /** The statement name, or an empty string for non-reusable, unnamed statement */
    public final String statementName;

    protected Prepared(Context ctx, T prevConn, String statementName) {
      super(ctx, prevConn);
      this.statementName = statementName;
    }

    /**
     * Bind the given params to the statement using the configured {@link ParamWriter}. This is the non-reusable
     * equivalent of {@link #bindReusable(String, Object...)}.
     */
    public CompletableFuture<Bound<T>> bind(Object... params) { return bindReusable("", params); }

    /**
     * Bind the given params to the statement using the configured {@link ParamWriter} and store the binding as a portal
     * name. The binding can be reused later in the same transaction via
     * {@link QueryReadyConnection.InTransaction#reuseBound(String)}. This defers to
     * {@link #bindReusableEx(String, boolean[], boolean[], Object...)}.
     */
    public CompletableFuture<Bound<T>> bindReusable(String portalName, Object... params) {
      return bindReusableEx(portalName, ctx.config.preferText ? FORMAT_TEXT_ALL : FORMAT_BINARY_ALL,
          ctx.config.preferText ? FORMAT_TEXT_ALL : FORMAT_BINARY_ALL, params);
    }

    /** The non-reusable form of {@link #bindReusableEx(String, boolean[], boolean[], Object...)} */
    public CompletableFuture<Bound<T>> bindEx(boolean[] paramsTextFormat, boolean[] resultsTextFormat,
        Object... params) {
      return bindReusableEx("", paramsTextFormat, resultsTextFormat, params);
    }

    /**
     * A more configurable form of {@link #bindReusable(String, Object...)}. The paramsTextFormat and resultsTextFormat
     * are booleans saying whether each param and result item format is or is not in text format. As a shortcut, if they
     * are empty arrays they represent text format for all values (see {@link #FORMAT_TEXT_ALL}. If they are
     * single-value arrays, the value is assumed to apply to all, so a single-value false array means all binary (see
     * {@link #FORMAT_BINARY_ALL}).
     */
    @SuppressWarnings("unchecked")
    public CompletableFuture<Bound<T>> bindReusableEx(String portalName, boolean[] paramsTextFormat,
        boolean[] resultsTextFormat, Object... params) {
      return sendBindWithConvertedParams(portalName, paramsTextFormat, resultsTextFormat, params).
          thenApply(__ -> new Bound(ctx, prevConn, this, portalName));
    }

    protected CompletableFuture<Void> sendBindWithConvertedParams(String portalName, boolean[] paramsTextFormat,
        boolean[] resultsTextFormat, Object... params) {
      ctx.buf.clear();
      ctx.writeByte((byte) 'B').writeLengthIntBegin().writeCString(portalName).writeCString(statementName).
          writeShort((short) paramsTextFormat.length);
      for (boolean paramTextFormat : paramsTextFormat) ctx.writeShort((short) (paramTextFormat ? 0 : 1));
      ctx.writeShort((short) params.length);
      for (int i = 0; i < params.length; i++) {
        if (params[i] == null) {
          ctx.writeInt(-1);
        } else {
          // Skip the length and write it after conversion
          ctx.buf.position(ctx.buf.position() + 4);
          int prevPos = ctx.buf.position();
          boolean textFormat = paramsTextFormat.length == 0 ||
              (paramsTextFormat.length == 1 && paramsTextFormat[0]) ||
              (paramsTextFormat.length > i &&  paramsTextFormat[i]);
          ctx.config.paramWriter.write(textFormat, params[i], ctx);
          ctx.buf.putInt(prevPos - 4, ctx.buf.position() - prevPos);
        }
      }
      ctx.writeShort((short) resultsTextFormat.length);
      for (boolean resultTextFormat : resultsTextFormat) ctx.writeShort((short) (resultTextFormat ? 0 : 1));
      ctx.writeLengthIntEnd();
      ctx.buf.flip();
      return writeFrontendMessage();
    }

    /** {@link #bind(Object...)} + {@link Bound#executeAndDone()} */
    public CompletableFuture<QueryResultConnection<T>> bindExecuteAndDone(Object... params) {
      return bind(params).thenCompose(Bound::executeAndDone);
    }

    /** {@link #bind(Object...)} + {@link Bound#describeExecuteAndDone()} */
    public CompletableFuture<QueryResultConnection<T>> bindDescribeExecuteAndDone(Object... params) {
      return bind(params).thenCompose(Bound::describeExecuteAndDone);
    }

    /** {@link #bind(Object...)} + {@link Bound#executeAndBack()} */
    public CompletableFuture<Prepared<T>> bindExecuteAndBack(Object... params) {
      return bind(params).thenCompose(Bound::executeAndBack);
    }

    /**
     * Send a "describe" request to the backend to describe the parameter and result types. When flushed or done, this
     * will return {@link QueryMessage.ParamMeta} and {@link QueryMessage.RowMeta} messages to the result. Usually,
     * unless parameter data is needed, {@link Bound#describe()} is preferred after {@link #bind(Object...)}.
     */
    @Override
    public CompletableFuture<Prepared<T>> describe() {
      return sendDescribe(false, statementName).thenApply(__ -> this);
    }

    /** Close this statement. This does not need to be called for non-reusable (i.e. "unnamed") prepared statements. */
    public CompletableFuture<Prepared<T>> closeStatement() {
      return sendClose(false, statementName).thenApply(__ -> this);
    }
  }

  /** Connection state once a statement has been bound with parameters */
  public static class Bound<T extends QueryReadyConnection<T>> extends QueryBuildConnection<T, Bound<T>> {
    protected final @Nullable Prepared<T> prepared;
    /** The bound portal name or empty string for non-reusable, unnamed bound portal */
    public final String portalName;

    protected Bound(Context ctx, T prevConn, @Nullable Prepared<T> prepared, String portalName) {
      super(ctx, prevConn);
      this.prepared = prepared;
      this.portalName = portalName;
    }

    protected CompletableFuture<Void> sendExecute(int maxRows) {
      ctx.buf.clear();
      ctx.writeByte((byte) 'E').writeLengthIntBegin().writeCString(portalName).writeInt(maxRows).writeLengthIntEnd();
      ctx.buf.flip();
      return writeFrontendMessage();
    }

    /** Execute the bound statement */
    public CompletableFuture<Bound<T>> execute() { return execute(0); }

    /**
     * Execute the bound statement only accepting a limited set of rows. {@link QueryResultConnection#isSuspended()}
     * will be true when the rows have been read up to the max. This can then be called again for more rows when reusing
     * the bound statement.
     */
    public CompletableFuture<Bound<T>> execute(int maxRows) { return sendExecute(maxRows).thenApply(__ -> this); }

    /**
     * Without executing anything further, go back to the prepared statement. Fails if this was created via
     * {@link QueryReadyConnection.InTransaction#reuseBound(String)}.
     */
    public CompletableFuture<Prepared<T>> back() {
      if (prepared == null)
        throw new IllegalStateException("Bound portal is being reused and not part of prepared statement");
      return CompletableFuture.completedFuture(prepared);
    }

    /** {@link #execute()} + {@link #back()} */
    public CompletableFuture<Prepared<T>> executeAndBack() { return execute().thenCompose(Bound::back); }

    /** {@link #execute()} + {@link #done()} */
    public CompletableFuture<QueryResultConnection<T>> executeAndDone() { return execute().thenCompose(Bound::done); }

    /**
     * Describe the bound statement. This is the often-preferred "describe" that is like {@link Prepared#describe()}
     * except it only returns result row metadata instead of also including parameter metadata. This should be called
     * before {@link #execute()} when result column metadata is needed (e.g. column names).
     */
    @Override
    public CompletableFuture<Bound<T>> describe() { return sendDescribe(true, portalName).thenApply(__ -> this); }

    /** {@link #describe()} + {@link #execute()} */
    public CompletableFuture<Bound<T>> describeAndExecute() { return describe().thenCompose(Bound::execute); }

    /** {@link #describe()} + {@link #executeAndBack()} */
    public CompletableFuture<Prepared<T>> describeExecuteAndBack() {
      return describe().thenCompose(Bound::executeAndBack);
    }

    /** {@link #describe()} + {@link #executeAndDone()} */
    public CompletableFuture<QueryResultConnection<T>> describeExecuteAndDone() {
      return describe().thenCompose(Bound::executeAndDone);
    }

    /** Close this bound portal. This does not need to be called for non-reusable (i.e. "unnamed") bound portals. */
    public CompletableFuture<Bound<T>> closePortal() { return sendClose(true, portalName).thenApply(__ -> this); }
  }
}
