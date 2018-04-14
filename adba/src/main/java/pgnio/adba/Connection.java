package pgnio.adba;

import jdk.incubator.sql2.ShardingKey;
import jdk.incubator.sql2.SqlException;
import jdk.incubator.sql2.Transaction;
import pgnio.Config;
import pgnio.QueryReadyConnection;

import java.util.Map;
import java.util.Set;

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
    return new Operation<Void>(this, this, () -> {
      if (lifecycle != Lifecycle.NEW && lifecycle != Lifecycle.NEW_INACTIVE)
        throw new SqlException("Invalid lifecycle", null, null, -1, null, -1);
      return pgnio.Connection.authed(config).whenComplete((pgConn, err) -> {
        if (err != null) {
          lifecycle = Lifecycle.CLOSED;
        } else {
          if (lifecycle == Lifecycle.NEW) lifecycle = Lifecycle.OPEN;
          else if (lifecycle == Lifecycle.NEW_INACTIVE) lifecycle = Lifecycle.INACTIVE;
          else throw new SqlException("Invalid lifecycle", null, null, -1, null, -1);
          this.pgConn = pgConn;
        }
      }).thenApply(__ -> null);
    });
  }

  @Override
  public Operation<Void> validationOperation(Validation depth) {
    return null;
  }

  @Override
  public Operation<Void> closeOperation() {
    return null;
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
        ConnectionProperty pgnioProp = ConnectionProperty.fromConnectionProperty(prop);
        if (pgnioProp != null) pgnioProp.apply(conf, val);
      });
      return new Connection(conf);
    }
  }
}
