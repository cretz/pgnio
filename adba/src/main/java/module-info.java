
module pgnio.adba {
  requires jdk.incubator.adba;
  exports pgnio.adba;
  provides jdk.incubator.sql2.DataSourceFactory with pgnio.adba.DataSourceFactory;
}