package asyncpg;

public abstract class DriverException extends RuntimeException {
  public DriverException() { super(); }
  public DriverException(String message) { super(message); }
  public DriverException(String message, Throwable cause) { super(message, cause); }
  public DriverException(Throwable cause) { super(cause); }
}
