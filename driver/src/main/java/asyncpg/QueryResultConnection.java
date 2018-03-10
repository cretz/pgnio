package asyncpg;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import java.util.stream.Collector;

public class QueryResultConnection<T extends StartedConnection> extends StartedConnection {

  public boolean suspended;

  protected final T prevConn;
  protected int queryCounter;
  protected QueryMessage.RowMeta lastRowMeta;
  protected boolean done;
  protected final boolean willEndWithDone;

  protected QueryResultConnection(ConnectionContext ctx, T prevConn, boolean willEndWithDone) {
    super(ctx);
    this.prevConn = prevConn;
    this.willEndWithDone = willEndWithDone;
  }

  // This consumes bytes from the buf. Result of null means "done"
  protected QueryMessage handleReadMessage() {
    char typ = (char) ctx.buf.get();
    ctx.buf.position(5);
    switch (typ) {
      // CloseComplete
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
        // Just skip this
        return new QueryMessage.CopyDone(queryCounter);
      // CopyData
      case 'd':
        byte[] copyBytes = new byte[ctx.buf.remaining()];
        ctx.buf.get(copyBytes);
        return new QueryMessage.CopyData(queryCounter, copyBytes);
      // PortalSuspended
      case 's':
        // This is "done" like ready-for-query
        suspended = true;
        return null;
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
        int spaceIndex = tag.indexOf(' ');
        // XXX: we don't support COPY on PG < 8.2, so we can assume all have row count
        if (spaceIndex == -1) throw new IllegalStateException("Row count not present for command complete");
        QueryMessage.Complete.QueryType queryType =
            QueryMessage.Complete.QueryType.valueOf(tag.substring(0, spaceIndex));
        tag = tag.substring(spaceIndex + 1);
        long insertedOid = 0;
        long rowCount;
        if (queryType == QueryMessage.Complete.QueryType.INSERT) {
          spaceIndex = tag.indexOf(' ');
          if (spaceIndex == -1) throw new IllegalStateException("Insert oid not present for command complete");
          insertedOid = Long.parseLong(tag.substring(0, spaceIndex));
          rowCount = Long.parseLong(tag.substring(spaceIndex + 1));
        } else {
          rowCount = Long.parseLong(tag);
        }
        return new QueryMessage.Complete(queryCounter, lastRowMeta, queryType, insertedOid, rowCount);
      // DataRow
      case 'D':
        byte[][] values = new byte[ctx.buf.getShort()][];
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
        done = true;
        return null;
      default: throw new IllegalArgumentException("Unrecognized query message type: " + typ);
    }
  }

  // Message is null when this is complete
  public CompletableFuture<QueryMessage> next() {
    if (done) return CompletableFuture.completedFuture(null);
    return readNonGeneralBackendMessage().thenApply(__ -> handleReadMessage()).whenComplete((msg, ex) -> {
      // Up the counter and remove the last meta if complete/errored
      if (msg == null || msg.isQueryEndingMessage() || ex instanceof DriverException.FromServer) {
        queryCounter++;
        lastRowMeta = null;
      }
      // Mark done if the message is null
      done = msg == null;
    });
  }

  // Will return null on "done" or wait...
  public CompletableFuture<QueryMessage> next(Predicate<QueryMessage> pred) {
    return next().thenCompose(msg -> pred.test(msg) ? CompletableFuture.completedFuture(msg) : next(pred));
  }

  // Finds the next message of given type, discarding others. Will end when "done" (returning null) or hang and wait.
  @SuppressWarnings("unchecked")
  public <U extends QueryMessage> CompletableFuture<U> next(Class<U> messageType) {
    return next(messageType::isInstance).thenApply(msg -> (U) msg);
  }

  @SuppressWarnings("unchecked")
  public <R, A> CompletableFuture<R> collectRows(Collector<? super QueryMessage.Row, A, R> collector) {
    return collectRows(collector.supplier(), collector.accumulator()).thenApply(c -> {
      if (!collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH))
        return collector.finisher().apply(c);
      return (R) c;
    });
  }

  public <R> CompletableFuture<R> collectRows(Supplier<R> supplier,
      BiConsumer<R, ? super QueryMessage.Row> accumulator) {
    R ret = supplier.get();
    return forEachRow(row -> accumulator.accept(ret, row)).thenApply(__ -> ret);
  }

  public <R> CompletableFuture<R> reduceRows(R identity, BiFunction<R, ? super QueryMessage.Row, R> accumulator) {
    AtomicReference<R> ret = new AtomicReference<>(identity);
    return forEachRow(row -> ret.set(accumulator.apply(ret.get(), row))).thenApply(__ -> ret.get());
  }

  // Only runs for the rows remaining in the current query. Call again for future queries.
  public CompletableFuture<Void> forEachRow(Consumer<QueryMessage.Row> fn) {
    return forEachRowAsync(row -> {
      fn.accept(row);
      return CompletableFuture.completedFuture(null);
    });
  }

  // Only runs for the rows remaining in the current query. Call again for future queries.
  public CompletableFuture<Void> forEachRowAsync(Function<QueryMessage.Row, CompletableFuture<Void>> fn) {
    return next().thenCompose(msg -> {
      // Keep going until complete, only handling rows
      if (msg == null || msg.isQueryEndingMessage()) return CompletableFuture.completedFuture(null);
      if (!(msg instanceof QueryMessage.Row)) return forEachRowAsync(fn);
      return fn.apply((QueryMessage.Row) msg).thenCompose(__ -> forEachRowAsync(fn));
    });
  }

  protected CompletableFuture<Void> sendCopyData(byte[] data) {
    ctx.buf.clear();
    ctx.writeByte((byte) 'd').writeLengthIntBegin().writeBytes(data).writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  public CompletableFuture<Void> copyInWrite(byte[]... data) {
    CompletableFuture<Void> ret = CompletableFuture.completedFuture(null);
    for (byte[] bytes : data) ret = ret.thenCompose(__ -> sendCopyData(bytes));
    return ret;
  }

  protected CompletableFuture<Void> sendCopyDone() {
    ctx.buf.clear();
    ctx.writeByte((byte) 'c').writeLengthIntBegin().writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  public CompletableFuture<Void> copyInComplete() { return sendCopyDone(); }

  protected CompletableFuture<Void> sendCopyFail(String message) {
    ctx.buf.clear();
    ctx.writeByte((byte) 'f').writeLengthIntBegin().writeString(message).writeLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  public CompletableFuture<Void> copyInFailed(String message) { return sendCopyFail(message); }

  public CompletableFuture<T> done() {
    if (!willEndWithDone) return CompletableFuture.completedFuture(prevConn);
    // If we need to, call done until it is actually done
    return next().thenCompose(msg -> {
      if (msg != null) return done();
      prevConn.invalid = false;
      return CompletableFuture.completedFuture(prevConn);
    });
  }
}
