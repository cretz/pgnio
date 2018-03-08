package asyncpg;

public class SuspendableConnection<T extends QueryReadyConnection<T>> extends StartedConnection {
  protected SuspendableConnection(ConnectionContext ctx) { super(ctx); }
}
