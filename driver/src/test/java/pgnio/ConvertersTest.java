package pgnio;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.DoubleAdder;

public class ConvertersTest extends DbTestBase {
  // Most tests for converters are in QueryTest

  @Test
  public void testNumberParamConverter() {
    DoubleAdder doubleAdder = new DoubleAdder();
    doubleAdder.add(1.2);
    doubleAdder.add(3.4);
    double val = withConnectionSync(conn ->
      conn.preparedQueryRows("SELECT $1::double precision", doubleAdder).
          thenApply(rows -> RowReader.DEFAULT.get(rows.get(0), 0, Double.class)));
    Assert.assertEquals(doubleAdder.doubleValue(), val, 0.0);
  }
}
