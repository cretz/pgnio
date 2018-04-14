package pgnio.adba;

public class Config extends pgnio.Config {
  public String networkServerValidationQuery = "SELECT 1";
  public String completeValidationQuery = "SELECT 1";

  public Config networkServerValidationQuery(String networkServerValidationQuery) {
    this.networkServerValidationQuery = networkServerValidationQuery;
    return this;
  }
  public Config completeValidationQuery(String completeValidationQuery) {
    this.completeValidationQuery = completeValidationQuery;
    return this;
  }
}
