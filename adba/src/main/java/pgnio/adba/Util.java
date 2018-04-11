package pgnio.adba;

import jdk.incubator.sql2.ConnectionProperty;

class Util {

  // TODO: find better homes for these once we know all of their uses

  static void assertValidPropertyArgument(ConnectionProperty property, Object value) {
    boolean valid;
    try {
      valid = property.validate(value);
    } catch (Exception e) { throw new IllegalArgumentException("Invalid value", e); }
    if (!valid) throw new IllegalArgumentException("Invalid value");
  }

  private Util() { }
}
