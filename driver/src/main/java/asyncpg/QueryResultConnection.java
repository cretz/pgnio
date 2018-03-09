package asyncpg;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import java.util.stream.Collector;

public class QueryResultConnection<T extends QueryReadyConnection<T>> extends StartedConnection {

  protected final T prevConn;
  protected int queryCounter;
  protected QueryMessage.RowMeta lastRowMeta;
  protected boolean done;

  protected QueryResultConnection(ConnectionContext ctx, T prevConn) {
    super(ctx);
    this.prevConn = prevConn;
  }

  // Message is null when this is complete
  public CompletableFuture<QueryMessage> next() {
    if (done) return CompletableFuture.completedFuture(null);
    return readNonGeneralBackendMessage().thenCompose(__ -> {
      char typ = (char) ctx.buf.get();
      ctx.buf.position(5);
      switch (typ) {
        // CopyDone
        case 'c':
          // Just skip this
          return next();
        // CopyData
        case 'd':
          byte[] copyBytes = new byte[ctx.buf.remaining()];
          ctx.buf.get(copyBytes);
          return CompletableFuture.completedFuture(new QueryMessage.CopyData(queryCounter, copyBytes));
        // EmptyQueryResponse
        case 'B':
          return CompletableFuture.completedFuture(new QueryMessage.EmptyQuery(queryCounter));
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
          return CompletableFuture.completedFuture(
              new QueryMessage.Complete(queryCounter, lastRowMeta, queryType, insertedOid, rowCount));
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
          return CompletableFuture.completedFuture(new QueryMessage.Row(queryCounter, lastRowMeta, values));
        // CopyInResponse
        case 'G':
        // CopyOutResponse
        case 'H':
        // CopyBothResponse
        case 'W':
          boolean text = ctx.buf.get() == 0;
          boolean[] columnsText = new boolean[ctx.buf.getShort()];
          for (int i = 0; i < columnsText.length; i++) columnsText[i] = ctx.buf.getShort() == 0;
          QueryMessage.BeginCopy.Direction dir = typ == 'G' ? QueryMessage.BeginCopy.Direction.IN :
              (typ == 'H' ? QueryMessage.BeginCopy.Direction.OUT : QueryMessage.BeginCopy.Direction.BOTH);
          return CompletableFuture.completedFuture(new QueryMessage.BeginCopy(queryCounter, dir, text, columnsText));
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
          return CompletableFuture.completedFuture(lastRowMeta);
        // ReadyForQuery
        case 'Z':
          updateReadyForQueryTransactionStatus();
          done = true;
          return CompletableFuture.completedFuture(null);
        default: throw new IllegalArgumentException("Unrecognized query message type: " + typ);
      }
    }).whenComplete((msg, ex) -> {
      // Up the counter and remove the last meta if complete or errored
      if (msg instanceof QueryMessage.EmptyQuery ||
          msg instanceof QueryMessage.Complete ||
          ex instanceof ServerException) {
        queryCounter++;
        lastRowMeta = null;
      }
    });
  }

  public <R, A> CompletableFuture<R> collectRows(Collector<? super QueryMessage.Row, A, R> collector) {
    return collectRows(collector.supplier(), collector.accumulator()).thenApply(c -> {
      if (!collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH))
        return collector.finisher().apply(c);
      @SuppressWarnings("unchecked")
      R finished = (R) c;
      return finished;
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
      // Keep going until complete/empty, skipping row desc, failing on anything else
      if (msg instanceof QueryMessage.EmptyQuery || msg instanceof QueryMessage.Complete)
        return CompletableFuture.completedFuture(null);
      if (msg instanceof QueryMessage.RowMeta) return forEachRowAsync(fn);
      if (!(msg instanceof QueryMessage.Row))
        throw new IllegalStateException("Only expecting row messages, got: " + msg.getClass().getSimpleName());
      return fn.apply((QueryMessage.Row) msg).thenCompose(__ -> forEachRowAsync(fn));
    });
  }

  public CompletableFuture<Void> copyInWrite(byte[]... data) {
    CompletableFuture<Void> ret = CompletableFuture.completedFuture(null);
    for (byte[] bytes : data) ret = ret.thenCompose(__ -> {
      ctx.buf.clear();
      ctx.bufWriteByte((byte) 'd').bufLengthIntBegin().bufWriteBytes(bytes).bufLengthIntEnd();
      ctx.buf.flip();
      return writeFrontendMessage();
    });
    return ret;
  }

  public CompletableFuture<Void> copyInComplete() {
    ctx.buf.clear();
    ctx.bufWriteByte((byte) 'c').bufLengthIntBegin().bufLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  public CompletableFuture<Void> copyInFailed(String message) {
    ctx.buf.clear();
    ctx.bufWriteByte((byte) 'f').bufLengthIntBegin().bufWriteString(message).bufLengthIntEnd();
    ctx.buf.flip();
    return writeFrontendMessage();
  }

  public CompletableFuture<T> done() {
    // Call done until it is actually done
    return next().thenCompose(msg -> msg == null ? CompletableFuture.completedFuture(prevConn) : done());
  }
}
