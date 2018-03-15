package asyncpg;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class SslConnectionTest extends DbTestBase {

  // Doesn't work yet
  @Test(expected = RuntimeException.class)
  public void testFallback() {
    int val = withConnectionSync(newDefaultConfig().ssl(null), conn ->
      conn.simpleQueryRows("SELECT 15").thenApply(r -> RowReader.DEFAULT.get(r.get(0), 0, Integer.class)));
    Assert.assertEquals(val, 15);
  }
}
