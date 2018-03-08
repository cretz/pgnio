package asyncpg;

public abstract class StartedConnection extends Connection {
  protected StartedConnection(ConnectionContext ctx) { super(ctx); }

  public Subscribable<Notification> notifications() { return ctx.notificationSubscribable; }

  public Integer getProcessId() { return ctx.processId; }
  public Integer getSecretKey() { return ctx.secretKey; }
}
