package asyncpg;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class Subscribable<T> {
  // Purposefully not thread safe
  protected final Set<Function<T, CompletableFuture<Void>>> subscriptions = new LinkedHashSet<>();

  public Function<T, CompletableFuture<Void>> subscribe(Function<T, CompletableFuture<Void>> cb) {
    if (!subscriptions.add(cb)) throw new IllegalArgumentException("Function already subscribed");
    return cb;
  }

  public boolean unsubscribe(Function<T, CompletableFuture<Void>> cb) {
    return subscriptions.remove(cb);
  }

  public CompletableFuture<Void> publish(T item) {
    // We choose to go one at a time instead of allOf here because of the synchronous reqs of the Connection
    CompletableFuture<Void> ret = CompletableFuture.completedFuture(null);
    for (Function<T, CompletableFuture<Void>> cb : subscriptions) ret = ret.thenCompose(__ -> cb.apply(item));
    return ret;
  }
}
