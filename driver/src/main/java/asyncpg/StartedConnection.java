package asyncpg;

public abstract class StartedConnection extends Connection {
  // True when inside a query or something similar
  protected boolean invalid;

  protected StartedConnection(ConnectionContext ctx) { super(ctx); }

  public Subscribable<Notification> notifications() { return ctx.notificationSubscribable; }

  public Integer getProcessId() { return ctx.processId; }
  public Integer getSecretKey() { return ctx.secretKey; }

  protected void assertValid() {
    if (invalid) throw new IllegalStateException("Not ready for queries");
  }
}
