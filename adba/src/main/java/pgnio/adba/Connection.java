package pgnio.adba;

import jdk.incubator.sql2.ShardingKey;
import jdk.incubator.sql2.SqlException;
import jdk.incubator.sql2.Transaction;
import pgnio.QueryReadyConnection;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class Connection extends OperationGroup<Object, Object> implements jdk.incubator.sql2.Connection {
  protected final Config config;
  protected final Map<jdk.incubator.sql2.ConnectionProperty, Object> properties;
  protected final Set<ConnectionLifecycleListener> lifecycleListeners = new HashSet<>();
  protected Lifecycle lifecycle = Lifecycle.NEW;
  protected QueryReadyConnection.AutoCommit pgConn;

  public Connection(Config config, Map<ConnectionProperty, Object> properties) {
    super(null, null);
    this.config = config;
    // The set of properties is the default properties + non-sensitive given properties overrides
    Map<ConnectionProperty, Object> props = new HashMap<>(ConnectionProperty.Property.PROPERTIES_WITH_DEFAULTS);
    properties.forEach((prop, val) -> { if (!prop.isSensitive()) props.put(prop, val); });
    this.properties = Collections.unmodifiableMap(props);
  }

  @Override
  public Operation<Void> connectOperation() {
    if (lifecycle != Lifecycle.NEW) throw new IllegalStateException("Invalid lifecycle");
    return new Operation<>(this, this, () -> {
      if (lifecycle != Lifecycle.NEW && lifecycle != Lifecycle.NEW_INACTIVE)
        throw new SqlException("Invalid lifecycle", null, null, -1, null, -1);
      return pgnio.Connection.authed(config).whenComplete((pgConn, err) -> {
        if (err != null) {
          setLifecycle(Lifecycle.CLOSED);
        } else {
          setLifecycle(lifecycle.connect());
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
    return new Operation<>(this, this, () -> {
      setLifecycle(lifecycle.close());
      return pgConn.terminate().thenAccept(__ -> setLifecycle(lifecycle.closed()));
    }).unskippable(true);
  }

  @Override
  public <S, T> OperationGroup<S, T> operationGroup() {
    if (!lifecycle.isActive()) throw new IllegalStateException("Invalid lifecycle");
    // TODO: do I need "this" operation group to be passed in below as the parent?
    return new OperationGroup<>(this, null);
  }

  @Override
  public Transaction transaction() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Connection registerLifecycleListener(ConnectionLifecycleListener listener) {
    if (!lifecycle.isActive()) throw new IllegalStateException("Invalid lifecycle");
    lifecycleListeners.add(listener);
    return this;
  }

  @Override
  public Connection deregisterLifecycleListener(ConnectionLifecycleListener listener) {
    if (!lifecycle.isActive()) throw new IllegalStateException("Invalid lifecycle");
    lifecycleListeners.remove(listener);
    return this;
  }

  @Override
  public Lifecycle getConnectionLifecycle() { return lifecycle; }

  @Override
  public Connection abort() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Map<jdk.incubator.sql2.ConnectionProperty, Object> getProperties() { return properties; }

  @Override
  public ShardingKey.Builder shardingKeyBuilder() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Connection activate() {
    if (!lifecycle.isOpen()) throw new IllegalStateException("Invalid lifecycle");
    setLifecycle(lifecycle.activate());
    return this;
  }

  @Override
  public Connection deactivate() {
    if (!lifecycle.isOpen()) throw new IllegalStateException("Invalid lifecycle");
    // TODO: reset the connection?
    setLifecycle(lifecycle.deactivate());
    return this;
  }

  protected void setLifecycle(Lifecycle lifecycle) {
    if (this.lifecycle != lifecycle) {
      Lifecycle prev = this.lifecycle;
      this.lifecycle = lifecycle;
      lifecycleListeners.forEach(l -> l.lifecycleEvent(this, prev, lifecycle));
    }
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
      Map<ConnectionProperty, Object> usedProps = new HashMap<>();
      properties.forEach((prop, val) -> {
        ConnectionProperty connProp = ConnectionProperty.fromConnectionProperty(prop);
        if (connProp != null) {
          usedProps.put(connProp, val);
          connProp.accept(conf, val);
        }
      });
      return new Connection(conf, usedProps);
    }
  }
}
