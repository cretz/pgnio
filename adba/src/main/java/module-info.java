
module pgnio.adba {
  requires jdk.incubator.adba;
  requires pgnio;
  requires static checker.qual;
  exports pgnio.adba;
  provides jdk.incubator.sql2.DataSourceFactory with pgnio.adba.DataSourceFactory;
}