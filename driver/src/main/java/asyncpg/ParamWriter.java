package asyncpg;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ParamWriter {

  // Keyed by class name
  public static final Map<String, Converters.From> DEFAULT_CONVERTERS =
      Collections.unmodifiableMap(Converters.loadAllFromConverters());
  public static final ParamWriter DEFAULT = new ParamWriter(DEFAULT_CONVERTERS, false);

  protected final Map<String, Converters.From> converters;

  public ParamWriter(Map<String, Converters.From> converterOverrides) {
    this(converterOverrides, true);
  }

  public ParamWriter(Map<String, Converters.From> converters, boolean prependDefaults) {
    Map<String, Converters.From> map;
    if (prependDefaults) {
      map = new HashMap<>(DEFAULT_CONVERTERS.size() + converters.size());
      map.putAll(DEFAULT_CONVERTERS);
    } else {
      map = new HashMap<>(converters.size());
    }
    map.putAll(converters);
    this.converters = Collections.unmodifiableMap(map);
  }

  @SuppressWarnings("unchecked")
  public void write(boolean textFormat, Object obj, BufWriter buf) {
    // We don't look up the class list here, we expect the map to have all exact instances
    Converters.From conv = converters.get(obj.getClass().getName());
    if (conv == null) throw new DriverException.NoConversion(obj.getClass());
    try {
      conv.convertFrom(textFormat, obj, buf);
    } catch (Exception e) { throw new DriverException.ConvertFromFailed(obj.getClass(), e); }
  }
}
