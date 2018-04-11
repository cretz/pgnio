package pgnio.adba;

import jdk.incubator.sql2.ConnectionProperty;

import java.util.Map;
import java.util.Set;

public abstract class Connection implements jdk.incubator.sql2.Connection {

  public static class Builder implements jdk.incubator.sql2.Connection.Builder {
    protected final Map<ConnectionProperty, Object> properties;
    protected final Set<ConnectionProperty> cannotOverride;
    protected boolean built;

    protected Builder(Map<ConnectionProperty, Object> properties, Set<ConnectionProperty> cannotOverride) {
      this.properties = properties;
      this.cannotOverride = cannotOverride;
    }

    @Override
    public Builder property(ConnectionProperty p, Object v) {
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
      // TODO
      return null;
    }
  }
}
