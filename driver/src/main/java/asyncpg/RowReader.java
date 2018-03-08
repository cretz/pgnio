package asyncpg;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Function;

public class RowReader {

  // Do not modify this after creation
  protected final IdentityHashMap<Class, Function<byte[], Object>> converters;

  public RowReader(Map<Class, Function<byte[], Object>> converters) {
    this.converters = new IdentityHashMap<>(converters.size());
    this.converters.putAll(converters);
  }

  public <T> T get(QueryMessage.Row row, int colIndex, Class<T> typ) {
    throw new UnsupportedOperationException();
  }
}
