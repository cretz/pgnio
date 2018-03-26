package pgnio;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Subscription management for out-of-band messages that may occur. This is not thread safe so calls here should only be
 * made within the context of the connection.
 */
public class Subscribable<T> {
  // Purposefully not thread safe
  protected final Set<Function<T, CompletableFuture<Void>>> subscriptions = new LinkedHashSet<>();

  /**
   * Subscribe to messages of the generic type. Connection work will not continue until resulting future is complete.
   * The given function is also returned and can be used for {@link #unsubscribe(Function)}.
   */
  public Function<T, CompletableFuture<Void>> subscribe(Function<T, CompletableFuture<Void>> cb) {
    if (!subscriptions.add(cb)) throw new IllegalArgumentException("Function already subscribed");
    return cb;
  }

  /** Remove the given function. Returns true if removed, false if it was never there. */
  public boolean unsubscribe(Function<T, CompletableFuture<Void>> cb) { return subscriptions.remove(cb); }

  /** Clear all subscriptions */
  public void unsubscribeAll() { subscriptions.clear(); }

  /** Publish the given item to any subscribers, returning a future that is completed when they all are */
  public CompletableFuture<Void> publish(T item) {
    // We choose to go one at a time instead of allOf here because of the synchronous reqs of the Connection
    CompletableFuture<Void> ret = CompletableFuture.completedFuture(null);
    for (Function<T, CompletableFuture<Void>> cb : subscriptions) ret = ret.thenCompose(__ -> cb.apply(item));
    return ret;
  }

  /** Representation of a Postgres notice */
  public static class Notice {
    /**
     * Collection of fields for this notice. It is not necessarily read-only but callers should not mutate. In many
     * cases, it is easier to derive the key to access a field by using a {@link Field#key} of a known field.
     */
    public final Map<Byte, String> fields;

    /** Create a notice with the given fields */
    public Notice(Map<Byte, String> fields) { this.fields = fields; }

    /** The severity of the notice, localized */
    public String getLocalizedSeverity() { return fields.getOrDefault(Field.LOCALIZED_SEVERITY.key, "<no severity>"); }

    /** The severity of the notice or {@link Severity#UNKNOWN} */
    public Severity getSeverity() {
      String severity = fields.get(Field.SEVERITY.key);
      if (severity == null) severity = getLocalizedSeverity();
      try {
        return Severity.valueOf(severity);
      } catch (IllegalArgumentException e) { return Severity.UNKNOWN; }
    }

    /** The notice code */
    public String getCode() { return fields.getOrDefault(Field.CODE.key, "<no code>"); }

    /** The notice message */
    public String getMessage() { return fields.getOrDefault(Field.MESSAGE.key, "<no message>"); }

    /** Log the notice to the given logger */
    public void log(Logger log) { log(log, null); }

    protected void log(Logger log, Connection.@Nullable Context ctx) {
      Level level = getSeverity().toLogLevel();
      if (!log.isLoggable(level)) return;
      if (ctx != null) log.log(level, "[{0}] {1}", new Object[] { ctx, this });
      else log.log(level, "{0}", this);
      if (log.isLoggable(Level.FINER)) {
        StringBuilder details = new StringBuilder();
        for (Field field : Field.values()) {
          String v = fields.get(field.key);
          if (v != null) details.append("\n  [").append(field).append("] ").append(v);
        }
        if (ctx != null) log.log(Level.FINER, "[{0}] {1}{2}", new Object[] { ctx, this, details });
        else log.log(Level.FINER, "{0}{1}", new Object[] { this, details });
      }
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Notice notice = (Notice) o;
      return Objects.equals(fields, notice.fields);
    }

    @Override
    public int hashCode() { return Objects.hash(fields); }

    @Override
    public String toString() { return getLocalizedSeverity() + ": " + getMessage() + " [" + getCode() + "]"; }

    /** Known severities of Postgres notice/error messages */
    public enum Severity {
      ERROR, FATAL, PANIC, WARNING, NOTICE, DEBUG, INFO, LOG, UNKNOWN;

      /** Suggested translation from severity to Java logging level */
      public Level toLogLevel() {
        switch (this) {
          case FATAL:
          case PANIC:
            return Level.SEVERE;
          case ERROR:
          case WARNING:
            return Level.WARNING;
          case INFO:
          case LOG:
            return Level.INFO;
          case NOTICE:
            return Level.FINE;
          case DEBUG:
          case UNKNOWN:
            return Level.FINER;
          default: throw new AssertionError();
        }
      }
    }

    /** Known notice/error fields */
    public enum Field {
      LOCALIZED_SEVERITY('S'),
      SEVERITY('V'),
      CODE('C'),
      MESSAGE('M'),
      DETAIL('S'),
      HINT('H'),
      POSITION('P'),
      INTERNAL_POSITION('p'),
      INTERNAL_QUERY('q'),
      WHERE('W'),
      SCHEMA('s'),
      TABLE('t'),
      COLUMN('c'),
      DATA_TYPE('d'),
      CONSTRAINT('n'),
      FILE('F'),
      LINE('L'),
      ROUTINE('R');

      /** The key representing the field */
      public final Byte key;

      Field(char key) { this.key = (byte) key; }
    }
  }

  /** Representation of a Postgres notification */
  public static class Notification {
    /** The process ID of the notifying backend process */
    public final int processId;
    /** The notification's channel */
    public final String channel;
    /** The notification contents */
    public final String payload;

    public Notification(int processId, String channel, String payload) {
      this.processId = processId;
      this.channel = channel;
      this.payload = payload;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Notification that = (Notification) o;
      return processId == that.processId &&
          Objects.equals(channel, that.channel) &&
          Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() { return Objects.hash(processId, channel, payload); }

    @Override
    public String toString() { return "[process=" + processId + ",channel=" + channel + ",payload=" + payload + "]"; }
  }

  /** Representation of a Postgres parameter status change */
  public static class ParameterStatus {
    /** The name of the parameter */
    public final String parameter;
    /** The new parameter value */
    public final String value;

    public ParameterStatus(String parameter, String value) {
      this.parameter = parameter;
      this.value = value;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ParameterStatus that = (ParameterStatus) o;
      return Objects.equals(parameter, that.parameter) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() { return Objects.hash(parameter, value); }

    @Override
    public String toString() { return parameter + "=" + value; }
  }
}
