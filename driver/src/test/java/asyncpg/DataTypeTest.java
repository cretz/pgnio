package asyncpg;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;

public class DataTypeTest extends TestBase {
  static final Field[] oidFields = Arrays.stream(DataType.class.getDeclaredFields()).
      filter(f -> Modifier.isStatic(f.getModifiers()) && f.getType() == Integer.TYPE).
      toArray(Field[]::new);

  @Test
  public void testNameForOid() throws Exception {
    for (Field oidField : oidFields)
      Assert.assertEquals(oidField.getName(), DataType.nameForOid((int) oidField.get(null)));
  }

  @Test
  public void testArrayComponentOid() throws Exception {
    for (Field oidField : oidFields)
      if (oidField.getName().endsWith("_ARRAY"))
        Assert.assertEquals(oidField.getName().substring(0, oidField.getName().length() - 6),
            DataType.nameForOid(DataType.arrayComponentOid((int) oidField.get(null))));
  }
}
