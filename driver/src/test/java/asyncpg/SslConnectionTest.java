package asyncpg;

import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class SslConnectionTest extends DbTestBase {
  @Test
  public void testTrustAll() throws Exception {
    Config conf = newDefaultConfig().ssl(true).sslContextOverride(trustAllSslContext());
    int val = withConnectionSync(conf, conn ->
      conn.simpleQueryRows("SELECT 15").thenApply(r -> RowReader.DEFAULT.get(r.get(0), 0, Integer.class)));
    Assert.assertEquals(val, 15);
  }

  @Test
  public void testTrustServerKey() throws Exception {
    Config conf = newDefaultConfig().ssl(true).sslContextOverride(trustServerKey());
    int val = withConnectionSync(conf, conn ->
        conn.simpleQueryRows("SELECT 16").thenApply(r -> RowReader.DEFAULT.get(r.get(0), 0, Integer.class)));
    Assert.assertEquals(val, 16);
  }

  protected SSLContext trustAllSslContext() throws KeyManagementException, NoSuchAlgorithmException {
    TrustManager trustMgr = new X509TrustManager() {
      @Override
      public void checkClientTrusted(X509Certificate[] chain, String authType) { }
      @Override
      public void checkServerTrusted(X509Certificate[] chain, String authType) { }
      @Override
      public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
    };
    SSLContext ctx = SSLContext.getInstance("TLS");
    ctx.init(null, new TrustManager[] { trustMgr }, null);
    return ctx;
  }

  protected SSLContext trustServerKey()
      throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
    // Load cert
    Certificate cert;
    try (InputStream is = getClass().getResourceAsStream("keys/server.crt")) {
      cert = CertificateFactory.getInstance("X.509").generateCertificate(is);
    }
    // Put in key store
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    keyStore.load(null, null);
    keyStore.setCertificateEntry("test-server", cert);
    // Trust it
    TrustManagerFactory trustMgrFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustMgrFactory.init(keyStore);
    // Build ctx
    SSLContext ctx = SSLContext.getInstance("TLS");
    ctx.init(null, trustMgrFactory.getTrustManagers(), null);
    return ctx;
  }
}
