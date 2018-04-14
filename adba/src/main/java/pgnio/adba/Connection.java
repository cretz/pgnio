package pgnio.adba;

import jdk.incubator.sql2.ShardingKey;
import jdk.incubator.sql2.SqlException;
import jdk.incubator.sql2.Transaction;
import pgnio.QueryReadyConnection;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class Connection extends OperationGroup<Object, Object> implements jdk.incubator.sql2.Connection {
  protected final Config config;
  protected Lifecycle lifecycle;
  protected QueryReadyConnection.AutoCommit pgConn;

  public Connection(Config config) {
    super(null, null);
    this.config = config;
    lifecycle = Lifecycle.NEW;
  }

  @Override
  public Operation<Void> connectOperation() {
    if (lifecycle != Lifecycle.NEW) throw new IllegalStateException("Invalid lifecycle");
    return new Operation<>(this, this, () -> {
      if (lifecycle != Lifecycle.NEW && lifecycle != Lifecycle.NEW_INACTIVE)
        throw new SqlException("Invalid lifecycle", null, null, -1, null, -1);
      return pgnio.Connection.authed(config).whenComplete((pgConn, err) -> {
        if (err != null) {
          lifecycle = Lifecycle.CLOSED;
        } else {
          lifecycle = lifecycle.connect();
          this.pgConn = pgConn;
        }
      }).thenApply(__ -> null);
    });
  }

  @Override
  public Operation<Void> validationOperation(Validation depth) {
    if (!lifecycle.isActive()) throw new IllegalStateException("Invalid lifecycle");
    return new Operation<>(this, this, () -> {
      switch (depth) {
        case NONE:
        case LOCAL:
        case SOCKET:
          if (!pgConn.isOpen()) throw new IllegalStateException("Connection not open");
          return CompletableFuture.completedFuture(null);
        case NETWORK:
        case SERVER:
          return pgConn.simpleQueryExec(config.networkServerValidationQuery).thenApply(__ -> null);
        case COMPLETE:
          return pgConn.simpleQueryExec(config.completeValidationQuery).thenApply(__ -> null);
        default: throw new AssertionError();
      }
    });
  }

  @Override
  public Operation<Void> closeOperation() {
    if (!lifecycle.isActive()) throw new IllegalStateException("Invalid lifecycle");
    Operation<Void> ret = new Operation<>(this, this, () -> {
      lifecycle = lifecycle.close();
      return pgConn.terminate().thenAccept(__ -> lifecycle = lifecycle.closed());
    });
    ret.unskippable = true;
    return ret;
  }

  @Override
  public <S, T> jdk.incubator.sql2.OperationGroup<S, T> operationGroup() {
    return null;
  }

  @Override
  public Transaction transaction() {
    return null;
  }

  @Override
  public jdk.incubator.sql2.Connection registerLifecycleListener(ConnectionLifecycleListener listener) {
    return null;
  }

  @Override
  public jdk.incubator.sql2.Connection deregisterLifecycleListener(ConnectionLifecycleListener listener) {
    return null;
  }

  @Override
  public Lifecycle getConnectionLifecycle() {
    return null;
  }

  @Override
  public jdk.incubator.sql2.Connection abort() {
    return null;
  }

  @Override
  public Map<jdk.incubator.sql2.ConnectionProperty, Object> getProperties() {
    return null;
  }

  @Override
  public ShardingKey.Builder shardingKeyBuilder() {
    return null;
  }

  @Override
  public jdk.incubator.sql2.Connection activate() {
    return null;
  }

  @Override
  public jdk.incubator.sql2.Connection deactivate() {
    return null;
  }

  public static class Builder implements jdk.incubator.sql2.Connection.Builder {
    protected final Map<jdk.incubator.sql2.ConnectionProperty, Object> properties;
    protected final Set<jdk.incubator.sql2.ConnectionProperty> cannotOverride;
    protected boolean built;

    protected Builder(Map<jdk.incubator.sql2.ConnectionProperty, Object> properties,
        Set<jdk.incubator.sql2.ConnectionProperty> cannotOverride) {
      this.properties = properties;
      this.cannotOverride = cannotOverride;
    }

    @Override
    public Builder property(jdk.incubator.sql2.ConnectionProperty p, Object v) {
      if (built) throw new IllegalStateException("Already built");
      if (cannotOverride.contains(p)) throw new IllegalArgumentException("Property cannot be overridden");
      Util.assertValidPropertyArgument(p, v);
      properties.put(p, v);
      cannotOverride.add(p);
      return this;
    }

    @Override
    public Connection build() {
      if (built) throw new IllegalStateException("Already built");
      built = true;
      Config conf = new Config();
      properties.forEach((prop, val) -> {
        ConnectionProperty connProp = ConnectionProperty.fromConnectionProperty(prop);
        if (connProp != null) connProp.accept(conf, val);
      });
      return new Connection(conf);
    }
  }
}
