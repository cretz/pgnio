package pgnio;

import java.util.concurrent.TimeUnit;

/** Base exception for common driver exceptions */
public abstract class DriverException extends RuntimeException {
  public DriverException() { super(); }
  public DriverException(String message) { super(message); }
  public DriverException(String message, Throwable cause) { super(message, cause); }
  public DriverException(Throwable cause) { super(cause); }

  /** Thrown when SSL is required but unsupported by the server */
  public static class ServerSslNotSupported extends DriverException {
    public ServerSslNotSupported() { super("SSL required but server does not support SSL"); }
  }

  /** Thrown when {@link Connection.Started#unsolicitedMessageTick(long, TimeUnit)} gets any non-general message */
  public static class NonGeneralMessageOnTick extends DriverException {
    public NonGeneralMessageOnTick(char typ) { super("Expected general message, but got message of type: " + typ); }
  }

  /** Thrown when asking {@link RowReader} to read a column that does not exist */
  public static class ColumnNotPresent extends DriverException {
    public ColumnNotPresent(String message) { super(message); }
  }

  /** Thrown when {@link RowReader} or {@link ParamWriter} cannot convert to/from a class */
  public static class NoConversion extends DriverException {
    public NoConversion(Class cls) { super("No conversion defined for " + cls); }
  }

  /** Thrown when {@link RowReader} needs row metadata but it is not available */
  public static class MissingRowMeta extends DriverException {
    public MissingRowMeta() {
      super("Row meta data required but is missing. " +
          "Usually caused by doing an advanced bound execute w/out a describe first.");
    }
  }

  /** Thrown when {@link RowReader} cannot convert an OID to a class */
  public static class InvalidConvertDataType extends DriverException {
    protected static String oidToString(int oid) {
      String name = DataType.nameForOid(oid);
      return name == null ? "data-type-#" + oid : name;
    }

    public InvalidConvertDataType(Class cls, int oid) { super("Cannot convert " + oidToString(oid) + " to " + cls); }
  }

  /** Thrown when {@link RowReader} fails to perform a conversion */
  public static class ConvertToFailed extends DriverException {
    public ConvertToFailed(Class cls, int oid, Throwable cause) {
      super("Failed converting " + InvalidConvertDataType.oidToString(oid) + " to " + cls, cause);
    }
  }

  /** Thrown when {@link ParamWriter} fails to perform a conversion */
  public static class ConvertFromFailed extends DriverException {
    public ConvertFromFailed(Class cls, Throwable cause) {
      super("Failed converting from " + cls, cause);
    }
  }

  /** Thrown when the Postgres server issues an error */
  public static class FromServer extends DriverException {
    /** The notice the server sent */
    public final Subscribable.Notice notice;

    public FromServer(Subscribable.Notice notice) {
      super(notice.toString());
      this.notice = notice;
    }
  }
}
