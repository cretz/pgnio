package pgnio.adba;

import jdk.incubator.sql2.ConnectionProperty;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DataSource implements jdk.incubator.sql2.DataSource {

  protected final Builder builder;

  protected DataSource(Builder builder) { this.builder = builder; }

  @Override
  public Connection.Builder builder() { return new Connection.Builder(builder.properties, builder.cannotOverride); }

  @Override
  public void close() { throw new UnsupportedOperationException("TODO: get clarification"); }

  public static class Builder implements jdk.incubator.sql2.DataSource.Builder {
    protected final Map<ConnectionProperty, Object> properties = new HashMap<>();
    protected final Set<ConnectionProperty> cannotOverride = new HashSet<>();
    protected boolean built;

    @Override
    public Builder defaultConnectionProperty(ConnectionProperty property, Object value) {
      if (built) throw new IllegalStateException("Already built");
      if (properties.containsKey(property)) throw new IllegalArgumentException("Property already set");
      Util.assertValidPropertyArgument(property, value);
      properties.put(property, value);
      return this;
    }

    @Override
    public Builder connectionProperty(ConnectionProperty property, Object value) {
      defaultConnectionProperty(property, value);
      cannotOverride.add(property);
      return this;
    }

    @Override
    public DataSource build() {
      if (built) throw new IllegalStateException("Already built");
      built = true;
      return new DataSource(this);
    }
  }
}
