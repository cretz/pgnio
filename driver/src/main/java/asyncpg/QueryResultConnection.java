package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import java.util.logging.Level;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/** Connection state to retrieve server responses after a query set is executed */
public class QueryResultConnection<T extends Connection.Started> extends Connection.Started {
  protected final T prevConn;
  protected int queryCounter;
  protected QueryMessage.@Nullable RowMeta lastRowMeta;
  protected @Nullable QueryMessage doneMessage;
  protected final boolean willEndWithDone;
  protected boolean copyInWaitingForComplete;
  protected boolean copyOutWaitingForComplete;

  protected QueryResultConnection(Context ctx, T prevConn, boolean willEndWithDone) {
    super(ctx);
    this.prevConn = prevConn;
    this.willEndWithDone = willEndWithDone;
  }

  /** True if this set of results is done */
  public boolean isDone() { return doneMessage != null; }

  /**
   * True if this set of results is done and suspended pending another call to
   * {@link QueryBuildConnection.Bound#execute(int)}
   */
  public boolean isSuspended() { return doneMessage instanceof QueryMessage.PortalSuspended; }

  /** Note, this consumes the buf */
  protected QueryMessage handleReadMessage() {
    char typ = (char) ctx.buf.get();
    ctx.buf.position(5);
    switch (typ) {
      // TODO: make some of these enums, only put query counters on the ones that need it
      // ParseComplete
      case '1':
        return new QueryMessage.ParseComplete(queryCounter);
      // BindComplete
      case '2':
        return new QueryMessage.BindComplete(queryCounter);
      // CloseComplete
      case '3':
        return new QueryMessage.CloseComplete(queryCounter);
      // CopyDone
      case 'c':
        copyOutWaitingForComplete = false;
        return new QueryMessage.CopyDone(queryCounter);
      // CopyData
      case 'd':
        byte[] copyBytes = new byte[ctx.buf.remaining()];
        ctx.buf.get(copyBytes);
        return new QueryMessage.CopyData(queryCounter, copyBytes);
      // NoData
      case 'n':
        return new QueryMessage.NoData(queryCounter);
      // PortalSuspended
      case 's':
        return new QueryMessage.PortalSuspended(queryCounter);
      // ParameterDescription
      case 't':
        int[] paramOids = new int[ctx.buf.getShort()];
        for (int i = 0; i < paramOids.length; i++) paramOids[i] = ctx.buf.getInt();
        return new QueryMessage.ParamMeta(queryCounter, paramOids);
      // EmptyQueryResponse
      case 'B':
        return new QueryMessage.EmptyQuery(queryCounter);
      // CommandComplete
      case 'C':
        String tag = ctx.bufReadString();
        log.log(Level.FINEST, "Command complete with tag: {0}", tag);
        return new QueryMessage.Complete(queryCounter, lastRowMeta, tag);
      // DataRow
      case 'D':
        byte[]@Nullable [] values = new byte[ctx.buf.getShort()][];
        for (int i = 0; i < values.length; i++) {
          int length = ctx.buf.getInt();
          if (length == -1) values[i] = null;
          else if (length == 0) values[i] = new byte[0];
          else {
            byte[] bytes = new byte[length];
            ctx.buf.get(bytes);
            values[i] = bytes;
          }
        }
        return new QueryMessage.Row(queryCounter, lastRowMeta, values);
      // CopyInResponse
      case 'G':
      // CopyOutResponse
      case 'H':
      // CopyBothResponse
      case 'W':
        boolean text = ctx.buf.get() == 0;
        boolean[] columnsText = new boolean[ctx.buf.getShort()];
        for (int i = 0; i < columnsText.length; i++) columnsText[i] = ctx.buf.getShort() == 0;
        QueryMessage.CopyBegin.Direction dir = typ == 'G' ? QueryMessage.CopyBegin.Direction.IN :
            (typ == 'H' ? QueryMessage.CopyBegin.Direction.OUT : QueryMessage.CopyBegin.Direction.BOTH);
        if (dir != QueryMessage.CopyBegin.Direction.IN) copyOutWaitingForComplete = true;
        if (dir != QueryMessage.CopyBegin.Direction.OUT) copyInWaitingForComplete = true;
        return new QueryMessage.CopyBegin(queryCounter, dir, text, columnsText);
      // RowDescription
      case 'T':
        short len = ctx.buf.getShort();
        QueryMessage.RowMeta.Column[] columns = new QueryMessage.RowMeta.Column[len];
        Map<String, QueryMessage.RowMeta.Column> columnsByName = new HashMap<>(len);
        for (int i = 0; i < len; i++) {
          QueryMessage.RowMeta.Column column = new QueryMessage.RowMeta.Column(
              i, ctx.bufReadString(), ctx.buf.getInt(), ctx.buf.getShort(), ctx.buf.getInt(),
              ctx.buf.getShort(), ctx.buf.getInt(), ctx.buf.getShort() == 0);
          columns[i] = column;
          columnsByName.put(column.name.toLowerCase(), column);
        }
        lastRowMeta = new QueryMessage.RowMeta(queryCounter, columns, columnsByName);
        return lastRowMeta;
      // ReadyForQuery
      case 'Z':
        updateReadyForQueryTransactionStatus();
        return new QueryMessage.ReadyForQuery(queryCounter);
      default: throw new IllegalArgumentException("Unrecognized query message type: " + typ);
    }
  }

  /** Get/wait for the next message or null if the set of results is done */
  @SuppressWarnings("return.type.incompatible")
  public CompletableFuture<@Nullable QueryMessage> next() {
    if (isDone()) return CompletableFuture.completedFuture(null);
    return readNonGeneralBackendMessage().thenApply(__ -> handleReadMessage()).whenComplete((msg, ex) -> {
      // Up the counter and remove the last meta if complete/errored
      if (msg.isQueryEndingMessage() || ex instanceof DriverException.FromServer) {
        queryCounter++;
        lastRowMeta = null;
      }
      if (msg.isQueryingDoneMessage()) doneMessage = msg;
    });
  }

  /** Repeatedly call {@link #next()} until the predicate is matched or the it is done (which returns null) */
  @SuppressWarnings("return.type.incompatible")
  public CompletableFuture<@Nullable QueryMessage> next(Predicate<QueryMessage> pred) {
    return next().thenCompose(msg ->
        msg == null || pred.test(msg) ? CompletableFuture.completedFuture(msg) : next(pred));
  }

  /** Shortcut for {@link #next(Predicate)} doing instanceof check */
  @SuppressWarnings("unchecked")
  public <U extends @Nullable QueryMessage> CompletableFuture<U> next(Class<U> messageType) {
    return next(messageType::isInstance).thenApply(msg -> (U) msg);
  }

  /** Shortcut for {@link #collectRows(Collector)} using {@link Collectors#toList()} */
  public CompletableFuture<List<QueryMessage.Row>> collectRows() { return collectRows(Collectors.toList()); }

  /** Shortcut for {@link #collectRows(Supplier, BiConsumer)} using the collector's supplier and accumulator */
  @SuppressWarnings("unchecked")
  public <R, A> CompletableFuture<R> collectRows(Collector<? super QueryMessage.Row, A, R> collector) {
    return collectRows(collector.supplier(), collector.accumulator()).thenApply(c -> {
      if (!collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH))
        return collector.finisher().apply(c);
      return (R) c;
    });
  }

  /** Creates item from supplier and calls {@link #forEachRow(Consumer)} using the accumulator */
  @SuppressWarnings("return.type.incompatible")
  public <R> CompletableFuture<R> collectRows(Supplier<R> supplier,
      BiConsumer<R, ? super QueryMessage.Row> accumulator) {
    R ret = supplier.get();
    return forEachRow(row -> accumulator.accept(ret, row)).thenApply(__ -> ret);
  }

  /** Run accumulator on {@link #forEachRow(Consumer)} constantly updating the initial value */
  public <R> CompletableFuture<R> reduceRows(R initial, BiFunction<R, ? super QueryMessage.Row, R> accumulator) {
    AtomicReference<R> ret = new AtomicReference<>(initial);
    return forEachRow(row -> ret.set(accumulator.apply(ret.get(), row))).thenApply(__ -> ret.get());
  }

  /**
   * Run the consumer for each remaining row in the current query. Call again for successive queries. Internally is just
   * the synchronous form of {@link #forEachRowAsync(Function)}.
   */
  public CompletableFuture<Void> forEachRow(Consumer<QueryMessage.Row> fn) {
    return forEachRowAsync(row -> {
      fn.accept(row);
      return CompletableFuture.completedFuture(null);
    });
  }

  /**
   * Run the given function for each remaining row in the query. Call again for successive queries. The next item will
   * not be fetched until the future returned by the previous call to the given function is complete.
   */
  public CompletableFuture<Void> forEachRowAsync(Function<QueryMessage.Row, CompletableFuture<Void>> fn) {
    return next().thenCompose(msg -> {
      // Keep going until complete, only handling rows
      if (msg == null || msg.isQueryEndingMessage()) return CompletableFuture.completedFuture(null);
      if (!(msg instanceof QueryMessage.Row)) return forEachRowAsync(fn);
      return fn.apply((QueryMessage.Row) msg).thenCompose(__ -> forEachRowAsync(fn));
    });
  }

  /**
   * Skip all messages until query complete and return the row count (can be rows selected or affected). Call again if
   * needed for successive queries. Will return null if this ends without a complete message or if it is complete with
   * a type of query that does not have result count. Essentially a shortcut for calling {@link #next(Class)} for
   * {@link QueryMessage.Complete}.
   */
  public CompletableFuture<@Nullable Long> collectRowCount() {
    return next(QueryMessage.Complete.class).thenApply(complete -> complete == null ? null : complete.getRowCount());
  }

  @Override
  protected CompletableFuture<QueryReadyConnection.AutoCommit> reset() { return done().thenCompose(Started::reset); }

  /**
   * Consume and ignore all remaining messages until the end of all queries for this query set is reached. Note, in some
   * cases such as a {@link QueryBuildConnection#flush()}, this will return immediately because it is known there is no
   * clear end-of-query-set marker. This returns the previous connection state.
   */
  public CompletableFuture<T> done() {
    // Send/wait for copy completes if necessary
    if (copyInWaitingForComplete || copyOutWaitingForComplete)
      return new Copy<>(ctx, this, copyInWaitingForComplete, copyOutWaitingForComplete).done();

    if (!willEndWithDone) return CompletableFuture.completedFuture(prevConn);
    // Consume all until done
    return next(__ -> false).thenApply(__ -> {
      prevConn.resumeControl();
      return prevConn;
    });
  }

  /** {@link #collectRows(Supplier, BiConsumer)} + {@link #done()} */
  public CompletableFuture<List<QueryMessage.Row>> collectRowsAndDone() {
    return collectRows().thenCompose(rows -> done().thenApply(__ -> rows));
  }

  /** {@link #collectRowCount()} + {@link #done()} */
  @SuppressWarnings("return.type.incompatible")
  public CompletableFuture<@Nullable Long> collectRowCountAndDone() {
    return collectRowCount().thenCompose(rowCount -> done().thenApply(__ -> rowCount));
  }

  /** Shortcut for {@link #copyIn(boolean)} that waits for begin acknowledgement */
  public CompletableFuture<Copy<T>> copyIn() { return copyIn(true); }

  /** Begin copy-in connection state, waiting for the copy-in-begin acknowledgement if param set to true */
  public CompletableFuture<Copy<T>> copyIn(boolean waitForBegin) {
    return copy(QueryMessage.CopyBegin.Direction.IN, waitForBegin);
  }

  /** Shortcut for {@link #copyOut(boolean)} that waits for begin acknowledgement */
  public CompletableFuture<Copy<T>> copyOut() { return copyOut(true); }

  /** Begin copy-out connection state, waiting for the copy-out-begin acknowledgement if param set to true */
  public CompletableFuture<Copy<T>> copyOut(boolean waitForBegin) {
    return copy(QueryMessage.CopyBegin.Direction.OUT, waitForBegin);
  }

  /** Shortcut for {@link #copyBoth(boolean)} that waits for begin acknowledgement */
  public CompletableFuture<Copy<T>> copyBoth() { return copyBoth(true); }

  /** Begin copy-both connection state, waiting for the copy-both-begin acknowledgement if param set to true */
  public CompletableFuture<Copy<T>> copyBoth(boolean waitForBegin) {
    return copy(QueryMessage.CopyBegin.Direction.BOTH, waitForBegin);
  }

  /** Begin copying in the given direction, optionally waiting for server begin acknowledgement */
  public CompletableFuture<Copy<T>> copy(QueryMessage.CopyBegin.Direction direction, boolean waitForBegin) {
    CompletableFuture<Void> ready =
        !waitForBegin ? CompletableFuture.completedFuture(null) : next(QueryMessage.CopyBegin.class).thenApply(msg -> {
          if (msg == null) throw new IllegalStateException("Expected copy begin, but never came");
          if (msg.direction != direction)
            throw new IllegalStateException("Expected copy " + direction + " got copy " + msg.direction);
          return null;
        });
    return ready.thenApply(__ ->
        new Copy<>(ctx, this,
            direction != QueryMessage.CopyBegin.Direction.OUT, direction != QueryMessage.CopyBegin.Direction.IN));
  }

  protected CompletableFuture<Void> sendCopyData(byte[] data) {
    ctx.buf.clear();
    ctx.writeByte((byte) 'd').writeLengthIntBegin().writeBytes(data).writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  /** Write raw bytes to server as part of COPY */
  public CompletableFuture<Void> copyInWrite(byte[]... data) {
    CompletableFuture<Void> ret = CompletableFuture.completedFuture(null);
    for (byte[] bytes : data) ret = ret.thenCompose(__ -> sendCopyData(bytes));
    return ret;
  }

  protected CompletableFuture<Void> sendCopyDone() {
    ctx.buf.clear();
    ctx.writeByte((byte) 'c').writeLengthIntBegin().writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage().thenRun(() -> copyInWaitingForComplete = false);
  }

  /** Send copy-in-done to server as part of COPY */
  public CompletableFuture<Void> copyInComplete() { return sendCopyDone(); }

  protected CompletableFuture<Void> sendCopyFail(String message) {
    ctx.buf.clear();
    ctx.writeByte((byte) 'f').writeLengthIntBegin().writeCString(message).writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage().thenRun(() -> copyInWaitingForComplete = false);
  }

  /** Send copy-in-fail to server as part of COPY */
  public CompletableFuture<Void> copyInFail(String message) { return sendCopyFail(message); }

  /** Connection state for copying data to/from the server */
  public static class Copy<T extends Connection.Started> extends Connection.Started {
    protected final QueryResultConnection<T> prevConn;
    /** Whether the server is accepting copy data from the client */
    public final boolean copyIn;
    /** Whether the client is accepting copy data from the server */
    public final boolean copyOut;
    protected boolean copyInComplete;
    protected boolean copyOutComplete;

    protected Copy(Context ctx, QueryResultConnection<T> prevConn, boolean copyIn, boolean copyOut) {
      super(ctx);
      this.prevConn = prevConn;
      this.copyIn = copyIn;
      this.copyOut = copyOut;
    }

    protected void assertCopyIn() { if (!copyIn) throw new IllegalStateException("Not in copy-in mode"); }

    /** Returns true if copy-in has been marked complete by this client. Fails if not copy-in mode. */
    public boolean isCopyInComplete() {
      assertCopyIn();
      return copyInComplete;
    }

    protected void assertNotCopyInComplete() {
      if (isCopyInComplete()) throw new IllegalStateException("Copy-in already completed");
    }

    /** Send data to server as part of copy-in. Fails if not copy-in mode. */
    public CompletableFuture<Copy<T>> sendData(byte[] bytes) {
      assertNotCopyInComplete();
      return prevConn.copyInWrite(bytes).thenApply(__ -> this);
    }

    /** Send copy-in complete and failed to server as part of copy-in. Fails if not copy-in mode. */
    public CompletableFuture<Copy<T>> sendFail(String message) {
      assertNotCopyInComplete();
      return prevConn.copyInFail(message).thenApply(__ -> {
        copyInComplete = true;
        return this;
      });
    }

    /** Send copy-in complete to server as part of copy-in. Fails if not copy-in mode. */
    public CompletableFuture<Copy<T>> sendComplete() {
      assertNotCopyInComplete();
      return prevConn.copyInComplete().thenApply(__ -> {
        copyInComplete = true;
        return this;
      });
    }

    protected void assertCopyOut() { if (!copyOut) throw new IllegalStateException("Not in copy-out mode"); }

    /** Returns true of copy-out is marked complete by server. Fails if not copy-out mode. */
    public boolean isCopyOutComplete() {
      assertCopyOut();
      return copyOutComplete;
    }

    /** Receive raw bytes from server during copy-out or null if done. Fails if not copy-out mode. */
    public CompletableFuture<byte@Nullable []> receiveData() {
      if (isCopyOutComplete()) return CompletableFuture.completedFuture(null);
      return prevConn.next(msg -> msg instanceof QueryMessage.CopyData || msg instanceof QueryMessage.CopyDone).
          thenApply(msg -> {
            if (msg instanceof QueryMessage.CopyData) return ((QueryMessage.CopyData) msg).bytes;
            copyOutComplete = true;
            return null;
          });
    }

    /**
     * Repeatedly call {@link #receiveData()} and pass to consumer until done. Fails if not copy-out mode. Synchronous
     * equivalent of {@link #receiveEachDataAsync(Function)}.
     */
    public CompletableFuture<Void> receiveEachData(Consumer<byte[]> fn) {
      return receiveEachDataAsync(data -> {
        fn.accept(data);
        return CompletableFuture.completedFuture(null);
      });
    }

    /**
     * Repeatedly call {@link #receiveData()} and pass to consumer until done. Fails if not copy-out mode. Next call is
     * not made until previous call's future is complete.
     */
    public CompletableFuture<Void> receiveEachDataAsync(Function<byte[], CompletableFuture<Void>> fn) {
      return receiveData().thenCompose(data -> {
        if (data == null) return CompletableFuture.completedFuture(null);
        return fn.apply(data).thenCompose(__ -> receiveEachDataAsync(fn));
      });
    }

    /** Essentially a "drain" that reads the copy-out messages until done. Fails if not copy-out-mode. */
    public CompletableFuture<Copy<T>> receiveIgnoreUntilComplete() {
      if (isCopyOutComplete()) return CompletableFuture.completedFuture(this);
      return receiveData().thenCompose(data ->
          data == null ? CompletableFuture.completedFuture(this) : receiveIgnoreUntilComplete());
    }

    /**
     * Complete the copy in either direction. If copy-in mode, sends copy done. If copy-out mode, calls
     * {@link #receiveIgnoreUntilComplete()}.
     */
    public CompletableFuture<QueryResultConnection<T>> complete() {
      // Send my complete if not sent
      CompletableFuture<Copy<T>> sentComplete =
          copyIn && !copyInComplete ? sendComplete() : CompletableFuture.completedFuture(this);
      return sentComplete.thenCompose(copy -> {
        if (!copyOut) return CompletableFuture.completedFuture(this);
        return receiveIgnoreUntilComplete();
      }).thenApply(copy -> copy.prevConn);
    }

    @Override
    protected CompletableFuture<QueryReadyConnection.AutoCommit> reset() { return done().thenCompose(Started::reset); }

    /** {@link #complete()} + {@link QueryResultConnection#done()} */
    public CompletableFuture<T> done() { return complete().thenCompose(QueryResultConnection::done); }
  }
}
