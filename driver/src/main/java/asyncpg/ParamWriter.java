package asyncpg;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ParamWriter {
  public static final ParamWriter DEFAULT;

  // Keyed by class name
  public static final Map<String, Converter> DEFAULT_CONVERTERS;

  static {
    Map<String, Converter> def = new HashMap<>();
    // TODO: add converters to def
    DEFAULT_CONVERTERS = Collections.unmodifiableMap(def);
    DEFAULT = new ParamWriter(def, false);
  }

  protected final Map<String, Converter> converters;

  public ParamWriter(Map<String, Converter> converterOverrides) {
    this(converterOverrides, true);
  }

  public ParamWriter(Map<String, Converter> converters, boolean prependDefaults) {
    Map<String, Converter> map;
    if (prependDefaults) {
      map = new HashMap<>(DEFAULT_CONVERTERS.size() + converters.size());
      map.putAll(DEFAULT_CONVERTERS);
    } else {
      map = new HashMap<>(converters.size());
    }
    map.putAll(converters);
    this.converters = Collections.unmodifiableMap(map);
  }

  // obj is never null
  public void write(boolean textFormat, Object obj, BufWriter buf) {
    // We don't look up the class list here, we expect the map to have all exact instances
    Converter conv = converters.get(obj.getClass().getName());
    if (conv == null) throw new DriverException.NoConversion(obj.getClass());
    conv.convert(textFormat, obj, buf);
  }

  @FunctionalInterface
  public interface Converter {
    void convert(boolean textFormat, Object obj, BufWriter buf);
  }
}
