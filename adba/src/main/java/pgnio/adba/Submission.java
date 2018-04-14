package pgnio.adba;

import jdk.incubator.sql2.SqlSkippedException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class Submission<T> implements jdk.incubator.sql2.Submission<T> {
  protected final Supplier<CompletableFuture<T>> operation;
  // Guaranteed to only be set once, never replaced after set
  protected final AtomicReference<Supplier<CompletableFuture<T>>> fut = new AtomicReference<>();

  public Submission(Supplier<CompletableFuture<T>> operation) { this.operation = operation; }

  @Override
  public CompletionStage<Boolean> cancel() {
    CompletableFuture<T> futureToCancel = new CompletableFuture<>();
    Supplier<CompletableFuture<T>> supplyFutureToCancel = () -> futureToCancel;
    if (!fut.compareAndSet(null, supplyFutureToCancel)) supplyFutureToCancel = fut.get();
    return CompletableFuture.completedStage(supplyFutureToCancel.get().completeExceptionally(
        new SqlSkippedException("Cancelled", null, null, -1, null, -1)));
  }

  @Override
  public CompletionStage<T> getCompletionStage() {
    Supplier<CompletableFuture<T>> supplyFuture = operation;
    if (!fut.compareAndSet(null, supplyFuture)) supplyFuture = fut.get();
    try {
      return supplyFuture.get();
    } catch (Exception e) {
      return CompletableFuture.failedStage(e);
    }
  }
}
