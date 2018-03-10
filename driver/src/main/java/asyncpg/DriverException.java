package asyncpg;

public abstract class DriverException extends RuntimeException {
  public DriverException() { super(); }
  public DriverException(String message) { super(message); }
  public DriverException(String message, Throwable cause) { super(message, cause); }
  public DriverException(Throwable cause) { super(cause); }

  public static class ColumnNotPresent extends DriverException {
    public ColumnNotPresent(String message) { super(message); }
  }

  public static class NoConversion extends DriverException {
    public NoConversion(Class cls) { super("No conversion defined for " + cls); }
  }

  public static class InvalidConvertDataType extends DriverException {
    protected static String oidToString(int oid) {
      String name = DataType.nameForOid(oid);
      return name == null ? String.valueOf(oid) : name;
    }

    public InvalidConvertDataType(Class cls, int oid) { super("Cannot convert " + cls + " to " + oidToString(oid)); }
  }

  public static class FromServer extends DriverException {
    public final Notice notice;

    public FromServer(Notice notice) {
      super(notice.toString());
      this.notice = notice;
    }
  }
}
