package asyncpg;

import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class Notice {

  public final Map<Byte, String> fields;

  public Notice(Map<Byte, String> fields) {
    this.fields = fields;
  }

  public String getLocalizedSeverity() { return fields.get(Field.LOCALIZED_SEVERITY.key); }

  public Severity getSeverity() {
    String severity = fields.get(Field.SEVERITY.key);
    if (severity == null) severity = getLocalizedSeverity();
    try {
      return Severity.valueOf(severity);
    } catch (IllegalArgumentException e) { return Severity.UNKNOWN; }
  }

  public String getCode() { return fields.get(Field.CODE.key); }

  public String getMessage() { return fields.get(Field.MESSAGE.key); }

  public void log(Logger log) { log(log, null); }

  protected void log(Logger log, ConnectionContext ctx) {
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
  public String toString() {
    return getLocalizedSeverity() + ": " + getMessage() + " [" + getCode() + "]";
  }

  public enum Severity {
    ERROR, FATAL, PANIC, WARNING, NOTICE, DEBUG, INFO, LOG, UNKNOWN;

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

    public final Byte key;

    Field(char key) { this.key = (byte) key; }
  }
}
