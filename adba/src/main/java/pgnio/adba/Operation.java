package pgnio.adba;

import jdk.incubator.sql2.Submission;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public class Operation<T> implements jdk.incubator.sql2.Operation<T> {
  protected final Connection conn;
  protected final OperationGroup<T, ?> group;
  protected Supplier<CompletableFuture<T>> operation;
  protected boolean submitted;
  protected boolean unskippable;

  @SuppressWarnings("unchecked")
  protected Operation(Connection conn, OperationGroup group, Supplier<CompletableFuture<T>> operation) {
    this.conn = conn == null ? (Connection) this : conn;
    this.group = group == null ? (OperationGroup<T, ?>) this : group;
    this.operation = operation == null ? (Supplier<CompletableFuture<T>>) this : operation;
  }

  protected void assertNotSubmitted() { if (submitted) throw new IllegalStateException("Already submitted"); }

  protected Operation<T> withOperationFut(UnaryOperator<CompletableFuture<T>> fn) {
    Supplier<CompletableFuture<T>> prevOperation = operation;
    operation = () -> fn.apply(prevOperation.get());
    return this;
  }

  @Override
  public Operation<T> onError(Consumer<Throwable> handler) {
    assertNotSubmitted();
    return withOperationFut(op -> op.whenComplete((__, err) -> { if (err != null) handler.accept(err); }));
  }

  @Override
  public Operation<T> timeout(Duration minTime) {
    assertNotSubmitted();
    long ns = minTime.toNanos();
    if (ns <= 0) throw new IllegalArgumentException("Timeout time <= 0");
    return withOperationFut(op -> op.orTimeout(ns, TimeUnit.NANOSECONDS));
  }

  @Override
  public Submission<T> submit() {
    assertNotSubmitted();
    submitted = true;
    return group.submitOperation(operation, unskippable);
  }

  public Operation<T> unskippable(boolean unskippable) {
    assertNotSubmitted();
    this.unskippable = unskippable;
    return this;
  }

  public static class Catch<T> extends Operation<T> {
    protected Catch(Connection conn, OperationGroup<T, ?> group) {
      super(conn, group, () -> CompletableFuture.completedFuture(null));
      unskippable(true);
    }

    @Override
    public Submission<T> submit() {
      withOperationFut(op -> op.exceptionally(__ -> null));
      return super.submit();
    }
  }
}
