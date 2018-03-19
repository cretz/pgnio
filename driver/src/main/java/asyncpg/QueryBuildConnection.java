package asyncpg;

import java.util.concurrent.CompletableFuture;

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

  protected CompletableFuture<Void> sendSync() {
    ctx.buf.clear();
    ctx.writeByte((byte) 'S').writeLengthIntBegin().writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  @Override
  protected CompletableFuture<QueryReadyConnection.AutoCommit> reset() {
    return done().thenCompose(QueryResultConnection::reset);
  }

  public CompletableFuture<QueryResultConnection<T>> done() {
    return sendSync().thenApply(__ -> new QueryResultConnection<>(ctx, prevConn, true));
  }

  public static class Prepared<T extends QueryReadyConnection<T>> extends QueryBuildConnection<T, Prepared<T>> {
    public static final boolean[] FORMAT_TEXT_ALL = new boolean[0];
    protected static final boolean[] FORMAT_BINARY_ALL = new boolean[] { false };
    public final String statementName;

    protected Prepared(Context ctx, T prevConn, String statementName) {
      super(ctx, prevConn);
      this.statementName = statementName;
    }

    public CompletableFuture<Bound<T>> describeAndBind(Object... params) {
      return describeStatement().thenCompose(c -> c.bind(params));
    }

    public CompletableFuture<Bound<T>> bind(Object... params) {
      return bindReusable("", params);
    }

    public CompletableFuture<Bound<T>> bindReusable(String portalName, Object... params) {
      return bindReusableEx(portalName, ctx.config.preferText ? FORMAT_TEXT_ALL : FORMAT_BINARY_ALL,
          ctx.config.preferText ? FORMAT_TEXT_ALL : FORMAT_BINARY_ALL, params);
    }

    public CompletableFuture<Bound<T>> bindEx(boolean[] paramsTextFormat, boolean[] resultsTextFormat,
        Object... params) {
      return bindReusableEx("", paramsTextFormat, resultsTextFormat, params);
    }

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

    public CompletableFuture<QueryResultConnection<T>> bindExecuteAndDone(Object... params) {
      return bind(params).thenCompose(Bound::executeAndDone);
    }

    public CompletableFuture<QueryResultConnection<T>> describeBindExecuteAndDone(Object... params) {
      return describeAndBind(params).thenCompose(Bound::executeAndDone);
    }

    public CompletableFuture<Prepared<T>> bindExecuteAndBack(Object... params) {
      return bind(params).thenCompose(Bound::executeAndBack);
    }

    public CompletableFuture<Prepared<T>> describeStatement() {
      return sendDescribe(false, statementName).thenApply(__ -> this);
    }

    public CompletableFuture<Prepared<T>> closeStatement() {
      return sendClose(false, statementName).thenApply(__ -> this);
    }

    public CompletableFuture<Bound<T>> reuseBound(String portalName) {
      return CompletableFuture.completedFuture(new Bound<>(ctx, prevConn, this, portalName));
    }
  }

  public static class Bound<T extends QueryReadyConnection<T>> extends QueryBuildConnection<T, Bound<T>> {
    protected final Prepared<T> prepared;
    public final String portalName;

    protected Bound(Context ctx, T prevConn, Prepared<T> prepared, String portalName) {
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

    public CompletableFuture<Bound<T>> execute() { return execute(0); }

    public CompletableFuture<Bound<T>> execute(int maxRows) {
      return sendExecute(maxRows).thenApply(__ -> this);
    }

    public CompletableFuture<Prepared<T>> back() {
      if (prepared == null) throw new IllegalStateException("This was not built from a prepared statement");
      return CompletableFuture.completedFuture(prepared);
    }

    public CompletableFuture<Prepared<T>> executeAndBack() {
      return execute().thenCompose(Bound::back);
    }

    public CompletableFuture<QueryResultConnection<T>> executeAndDone() {
      return execute().thenCompose(Bound::done);
    }

    public CompletableFuture<Bound<T>> describePortal() {
      return sendDescribe(true, portalName).thenApply(__ -> this);
    }

    public CompletableFuture<Bound<T>> closePortal() {
      return sendClose(true, portalName).thenApply(__ -> this);
    }
  }
}
