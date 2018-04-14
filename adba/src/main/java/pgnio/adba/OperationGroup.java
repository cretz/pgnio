package pgnio.adba;

import jdk.incubator.sql2.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collector;

public class OperationGroup<S, T> extends Operation<T>
    implements jdk.incubator.sql2.OperationGroup<S, T>, Supplier<CompletableFuture<T>> {

  protected static Collector DEFAULT_COLLECTOR = Collector.of(() -> null, (a, t) -> { }, (l, r) -> null, a -> null);

  protected boolean parallel;
  protected boolean independent;
  protected boolean submitted;
  protected CompletionStage<Boolean> condition;
  protected boolean held;
  protected Collector<S, ?, T> collector;

  protected OperationGroup(Connection conn, OperationGroup<T, ?> group) { super(conn, group, null); }

  @Override
  public CompletableFuture<T> get() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public OperationGroup<S, T> parallel() {
    assertNotSubmitted();
    parallel = true;
    return this;
  }

  @Override
  public OperationGroup<S, T> independent() {
    assertNotSubmitted();
    independent = true;
    return this;
  }

  @Override
  public OperationGroup<S, T> conditional(CompletionStage<Boolean> condition) {
    assertNotSubmitted();
    this.condition = condition;
    return this;
  }

  @Override
  public Submission<T> submitHoldingForMoreMembers() {
    assertNotSubmitted();
    held = true;
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public OperationGroup<S, T> releaseProhibitingMoreMembers() {
    assertNotSubmitted();
    held = false;
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public OperationGroup<S, T> collect(Collector<S, ?, T> c) {
    assertNotSubmitted();
    if (collector != null) throw new IllegalStateException("Collector already set");
    collector = c;
    return this;
  }

  @Override
  public Operation.Catch<S> catchOperation() { return new Operation.Catch<>(conn, this); }

  @Override
  public <R extends S> ArrayCountOperation<R> arrayCountOperation(String sql) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public <R extends S> ParameterizedCountOperation<R> countOperation(String sql) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Operation<S> operation(String sql) {
    return null;
  }

  @Override
  public <R extends S> OutOperation<R> outOperation(String sql) {
    return null;
  }

  @Override
  public <R extends S> ParameterizedRowOperation<R> rowOperation(String sql) {
    return null;
  }

  @Override
  public <R extends S> RowProcessorOperation<R> rowProcessorOperation(String sql) {
    return null;
  }

  @Override
  public <R extends S> StaticMultiOperation<R> staticMultiOperation(String sql) {
    return null;
  }

  @Override
  public <R extends S> DynamicMultiOperation<R> dynamicMultiOperation(String sql) {
    return null;
  }

  @Override
  public jdk.incubator.sql2.Operation endTransactionOperation(Transaction trans) {
    return null;
  }

  @Override
  public <R extends S> LocalOperation<R> localOperation() {
    return null;
  }

  @Override
  public <R extends S> Flow.Processor<jdk.incubator.sql2.Operation<R>, jdk.incubator.sql2.Submission<R>> operationProcessor() {
    return null;
  }

  @Override
  public jdk.incubator.sql2.OperationGroup<S, T> logger(Logger logger) {
    return null;
  }

  @Override
  public OperationGroup<S, T> onError(Consumer<Throwable> handler) {
    super.onError(handler);
    return this;
  }

  @Override
  public OperationGroup<S, T> timeout(Duration minTime) {
    super.timeout(minTime);
    return this;
  }

  protected Submission<S> submitOperation(Supplier<CompletableFuture<S>> operation, boolean unskippable) {
    throw new UnsupportedOperationException("TODO");
  }
}
