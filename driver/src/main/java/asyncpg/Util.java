package asyncpg;

import java.nio.channels.CompletionHandler;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class Util {

  @SuppressWarnings("type.argument.type.incompatible") // TODO
  public static final ThreadLocal<CharsetDecoder> threadLocalStringDecoder = ThreadLocal.withInitial(() ->
      StandardCharsets.UTF_8.newDecoder().
          onMalformedInput(CodingErrorAction.REPORT).
          onUnmappableCharacter(CodingErrorAction.REPORT));
  @SuppressWarnings("type.argument.type.incompatible") // TODO
  public static final ThreadLocal<CharsetEncoder> threadLocalStringEncoder = ThreadLocal.withInitial(() ->
      StandardCharsets.UTF_8.newEncoder().
          onMalformedInput(CodingErrorAction.REPORT).
          onUnmappableCharacter(CodingErrorAction.REPORT));

  public static <V> CompletionHandler<V, Void> handlerFromFuture(CompletableFuture<V> fut) {
    return new CompletionHandler<V, Void>() {
      @Override
      public void completed(V result, Void attachment) { fut.complete(result); }

      @Override
      public void failed(Throwable exc, Void attachment) { fut.completeExceptionally(exc); }
    };
  }

  public static <V, A> CompletionHandler<V, A> successHandler(BiConsumer<V, A> fn) {
    return new CompletionHandler<V, A>() {
      @Override
      public void completed(V result, A attachment) { fn.accept(result, attachment); }

      @Override
      public void failed(Throwable exc, A attachment) {
        if (exc instanceof RuntimeException) throw (RuntimeException) exc;
        throw new RuntimeException(exc);
      }
    };
  }

  static final char[] hexArray = "0123456789abcdef".toCharArray();

  public static String bytesToHex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2];
    for ( int j = 0; j < bytes.length; j++ ) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = hexArray[v >>> 4];
      hexChars[j * 2 + 1] = hexArray[v & 0x0F];
    }
    return new String(hexChars);
  }

  public static byte[] hexToBytes(String hex) {
    int len = hex.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2)
      data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4) + Character.digit(hex.charAt(i + 1), 16));
    return data;
  }

  public static byte[] md5Hex(MessageDigest md5, byte[]... byteArrays) {
    for (byte[] byteArray : byteArrays) md5.update(byteArray);
    byte[] digest = md5.digest();
    byte[] ret = new byte[digest.length * 2];
    for (int i = 0; i < digest.length; i++) {
      int v = digest[i] & 0xFF;
      ret[i * 2] = (byte) hexArray[v >>> 4];
      ret[i * 2 + 1] = (byte) hexArray[v & 0x0F];
    }
    return ret;
  }

  private Util() { }
}
