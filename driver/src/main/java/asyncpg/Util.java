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

  public static final ThreadLocal<CharsetDecoder> threadLocalStringDecoder = ThreadLocal.withInitial(() ->
      StandardCharsets.UTF_8.newDecoder().
          onMalformedInput(CodingErrorAction.REPORT).
          onUnmappableCharacter(CodingErrorAction.REPORT));
  public static final ThreadLocal<CharsetEncoder> threadLocalStringEncoder = ThreadLocal.withInitial(() ->
      StandardCharsets.UTF_8.newEncoder().
          onMalformedInput(CodingErrorAction.REPORT).
          onUnmappableCharacter(CodingErrorAction.REPORT));

  static final char[] hexChars = "0123456789abcdef".toCharArray();
  public static byte[] md5Hex(MessageDigest md5, byte[]... byteArrays) {
    for (byte[] byteArray : byteArrays) md5.update(byteArray);
    byte[] digest = md5.digest();
    byte[] ret = new byte[digest.length * 2];
    for (int i = 0; i < digest.length; i++) {
      int v = digest[i] & 0xFF;
      ret[i * 2] = (byte) hexChars[v >>> 4];
      ret[i * 2 + 1] = (byte) hexChars[v & 0x0F];
    }
    return ret;
  }

  private Util() { }
}
