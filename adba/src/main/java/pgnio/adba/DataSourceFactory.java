package pgnio.adba;

public class DataSourceFactory implements jdk.incubator.sql2.DataSourceFactory {
  @Override
  public DataSource.Builder builder() { return new DataSource.Builder(); }
}
