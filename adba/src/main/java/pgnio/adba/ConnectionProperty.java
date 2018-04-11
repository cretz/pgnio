package pgnio.adba;

import jdk.incubator.sql2.AdbaConnectionProperty;
import jdk.incubator.sql2.Operation;
import jdk.incubator.sql2.OperationGroup;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface ConnectionProperty
    extends jdk.incubator.sql2.ConnectionProperty, BiFunction<pgnio.Config, Object, pgnio.Config> {

  static @Nullable ConnectionProperty fromConnectionProperty(jdk.incubator.sql2.ConnectionProperty property) {
    if (property instanceof ConnectionProperty) return (ConnectionProperty) property;
    if (!(property instanceof AdbaConnectionProperty)) return null;
    switch ((AdbaConnectionProperty) property) {
      case PASSWORD: return Property.PASSWORD;
      // TODO: others
      default: return null;
    }
  }

  enum Property implements ConnectionProperty {
    HOSTNAME("hostname"),
    PORT("port"),
    USERNAME("username"),
    PASSWORD("password"),
    DATABASE("database"),
    SSL("ssl"),
    DEFAULT_TIMEOUT("defaultTimeout"),
    DEFAULT_TIMEOUT_UNIT("defaultTimeoutUnit"),
    DIRECT_BUFFER("directBuffer"),
    BUFFER_STEP("bufferStep"),
    PROTOCOL_VERSION("protocolVersion"),
    ADDITIONAL_STARTUP_PARAMS("additionalStartupParams"),
    LOG_NOTICES("logNotices"),
    PREFER_TEXT("preferText"),
    PARAM_WRITER("paramWriter"),
    POOL_SIZE("poolSize"),
    POOL_VALIDATION_QUERY("poolValidationQuery"),
    POOL_CONNECT_EAGERLY("poolConnectEagerly"),
    POOL_CLOSE_RETURNED_CONNECTION_ON_CLOSED_POOL("poolCloseReturnedConnectionOnClosedPool"),
    CONNECTOR("connector"),
    IO_CONNECTOR("ioConnector"),
    SSL_CONTEXT_OVERRIDE("sslContextOverride"),
    SSL_WRAPPER("sslWrapper");

    protected final ConnectionProperty delegate;

    Property(String fieldName) { delegate = Simple.fromConfigField(name(), fieldName); }

    @Override
    public pgnio.Config apply(pgnio.Config config, Object o) { return delegate.apply(config, o); }
    @Override
    public Class<?> range() { return delegate.range(); }
    @Override
    public boolean validate(Object value) { return delegate.validate(value); }
    @Override
    public Object defaultValue() { return delegate.defaultValue(); }
    @Override
    public boolean isSensitive() { return delegate.isSensitive(); }
    @Override
    public <S> Operation<? extends S> configureOperation(OperationGroup<S, ?> group, Object value) {
      return delegate.configureOperation(group, value);
    }
  }

  class Simple implements ConnectionProperty {
    protected static final pgnio.Config defaultConfig = new pgnio.Config();
    @SuppressWarnings("unchecked")
    protected static Simple fromConfigField(String name, String fieldName) {
      try {
        Field field = ConnectionProperty.class.getDeclaredField(fieldName);
        Class<?> range = field.getType().isPrimitive() ?
            pgnio.Util.boxedClassFromPrimitive(field.getType()) : field.getType();
        MethodType setterType = MethodType.methodType(ConnectionProperty.class, field.getType());
        MethodHandles.Lookup setterLookup = MethodHandles.lookup();
        MethodHandle setter = setterLookup.findVirtual(ConnectionProperty.class, fieldName, setterType);
        BiFunction<pgnio.Config, Object, pgnio.Config> apply = (BiFunction<pgnio.Config, Object, pgnio.Config>)
            LambdaMetafactory.metafactory(setterLookup, fieldName, MethodType.methodType(BiFunction.class),
                setterType.generic(), setter, setterType).getTarget().invokeExact();
        Object defaultValue = field.get(defaultConfig);
        return new Simple(name, range, apply, defaultValue, null, "password".equals(fieldName));
      } catch (Throwable e) {
        throw new IllegalArgumentException("Invalid field: " + fieldName, e);
      }
    }

    protected final String name;
    protected final Class<?> range;
    protected final BiFunction<pgnio.Config, Object, pgnio.Config> apply;
    protected final @Nullable Object defaultValue;
    protected final @Nullable Function<Object, Boolean> validator;
    protected final boolean sensitive;

    public Simple(String name, Class<?> range, BiFunction<pgnio.Config, Object, pgnio.Config> apply,
        @Nullable Object defaultValue, @Nullable Function<Object, Boolean> validator, boolean sensitive) {
      this.name = name;
      this.range = range;
      this.apply = apply;
      this.defaultValue = defaultValue;
      this.validator = validator;
      this.sensitive = sensitive;
    }

    @Override
    public pgnio.Config apply(pgnio.Config config, Object value) { return apply.apply(config, value); }
    @Override
    public String name() { return name; }
    @Override
    public Class<?> range() { return range; }
    @Override
    public boolean validate(Object value) { return validator == null || validator.apply(value); }
    @Override
    public Object defaultValue() { return defaultValue; }
    @Override
    public boolean isSensitive() { return sensitive; }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Simple simple = (Simple) o;
      return sensitive == simple.sensitive &&
          Objects.equals(name, simple.name) &&
          Objects.equals(range, simple.range) &&
          Objects.equals(apply, simple.apply) &&
          Objects.equals(defaultValue, simple.defaultValue) &&
          Objects.equals(validator, simple.validator);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, range, apply, defaultValue, validator, sensitive);
    }
  }
}
