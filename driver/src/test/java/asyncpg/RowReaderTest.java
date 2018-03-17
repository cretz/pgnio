package asyncpg;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class RowReaderTest extends TestBase {
  static final QueryMessage.RowMeta.Column fakeCol =
      new QueryMessage.RowMeta.Column(0, "test", 0, (short) 0, 0, (short) 0, 0, true);

  @Test
  public void testSimpleHStore() {
    Map<String, String> expected = new HashMap<>();
    expected.put("foo", "bar");
    expected.put("baz", null);
    Assert.assertEquals(expected, RowReader.DEFAULT.get(fakeCol, "foo=>bar, baz=>NULL".getBytes(), Map.class));
  }

  static Map<Integer, double[]> genericMap1() {
    Map<Integer, double[]> map = new HashMap<>();
    map.put(5, new double[] { 1.2, 3.4 });
    map.put(6, null);
    return map;
  }

  /*
  TODO: Support "type"
  @Test
  public void testGenericHStore() throws Exception {
    Assert.assertEquals(genericMap1(), RowReader.DEFAULT.get(fakeCol,
        "5=>\"{1.2,3.4}\", 6=>NULL".getBytes(), getClass().getMethod("genericMap1").getGenericReturnType()));
  }
  */
}
