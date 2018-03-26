package pgnio;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.charset.*;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Utilities used withing the library that may have use for others */
public class Util {
  @SuppressWarnings("type.argument.type.incompatible")
  private static final ThreadLocal<CharsetDecoder> threadLocalStringDecoder = ThreadLocal.withInitial(() ->
      StandardCharsets.UTF_8.newDecoder().
          onMalformedInput(CodingErrorAction.REPORT).
          onUnmappableCharacter(CodingErrorAction.REPORT));
  @SuppressWarnings("type.argument.type.incompatible")
  private static final ThreadLocal<CharsetEncoder> threadLocalStringEncoder = ThreadLocal.withInitial(() ->
      StandardCharsets.UTF_8.newEncoder().
          onMalformedInput(CodingErrorAction.REPORT).
          onUnmappableCharacter(CodingErrorAction.REPORT));

  // TODO: there's room for wins on some of these to reuse char/byte buffers for decoding

  /** Shortcut for {@link #charBufferFromByteBuffer(ByteBuffer)} */
  public static String stringFromBytes(byte[] bytes) { return stringFromByteBuffer(ByteBuffer.wrap(bytes)); }
  /** Shortcut for {@link #charBufferFromByteBuffer(ByteBuffer)} */
  public static String stringFromByteBuffer(ByteBuffer bytes) { return charBufferFromByteBuffer(bytes).toString(); }
  /** Shortcut for {@link #charBufferFromByteBuffer(ByteBuffer)} */
  public static char[] charsFromBytes(byte[] bytes) {
    CharBuffer buf = charBufferFromByteBuffer(ByteBuffer.wrap(bytes));
    if (buf.limit() == buf.capacity()) return buf.array();
    return Arrays.copyOf(buf.array(), buf.limit());
  }
  /**
   * Get characters from the given byte buffer using UTF-8 decoding. Unlike {@link String#String(byte[], Charset)} and
   * others, this does error on malformed input or unmappable characters.
   */
  public static CharBuffer charBufferFromByteBuffer(ByteBuffer bytes) {
    try {
      return threadLocalStringDecoder.get().decode(bytes);
    } catch (CharacterCodingException e) { throw new RuntimeException(e); }
  }

  /** Shortcut for {@link #byteBufferFromCharBuffer(CharBuffer)} */
  public static byte[] bytesFromString(String str) { return bytesFromByteBuffer(byteBufferFromString(str)); }
  /** Shortcut for {@link #byteBufferFromCharBuffer(CharBuffer)} */
  public static byte[] bytesFromCharBuffer(CharBuffer buf) {
    return bytesFromByteBuffer(byteBufferFromCharBuffer(buf));
  }
  /**
   * Shortcut for {@link #byteBufferFromCharBuffer(CharBuffer)}. Note, this may return the byte buffer's backing array
   * so it is assumed the buf is never used again.
   */
  protected static byte[] bytesFromByteBuffer(ByteBuffer buf) {
    if (buf.limit() == buf.capacity()) return buf.array();
    return Arrays.copyOf(buf.array(), buf.limit());
  }
  /** Shortcut for {@link #byteBufferFromCharBuffer(CharBuffer)} */
  public static ByteBuffer byteBufferFromString(String str) { return byteBufferFromCharBuffer(CharBuffer.wrap(str)); }
  /**
   * Get bytes from the given char buffer. Unlike {@link String#getBytes(Charset)} and others, this does error on
   * malformed input or unmappable characters.
   */
  public static ByteBuffer byteBufferFromCharBuffer(CharBuffer buf) {
    try {
      return threadLocalStringEncoder.get().encode(buf);
    } catch (CharacterCodingException e) { throw new RuntimeException(e); }
  }

  /**
   * Turn a not-yet-done completable future to a completion handler. This populates the future's complete or exception
   * based on the handler's complete or exception.
   */
  public static <V> CompletionHandler<V, Void> handlerFromFuture(CompletableFuture<V> fut) {
    return new CompletionHandler<V, Void>() {
      @Override
      public void completed(V result, Void attachment) { fut.complete(result); }

      @Override
      public void failed(Throwable exc, Void attachment) { fut.completeExceptionally(exc); }
    };
  }

  static final char[] hexArray = "0123456789abcdef".toCharArray();

  /** Convert a single byte to a 2-char hex string */
  public static String byteToHex(byte b) { return new String(new char[] { hexArray[b >>> 4], hexArray[b & 0x0F] }); }
  /** Convert a byte array to a hex string */
  public static String bytesToHex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = hexArray[v >>> 4];
      hexChars[j * 2 + 1] = hexArray[v & 0x0F];
    }
    return new String(hexChars);
  }

  /** Convert a two-char hex string to a single byte */
  public static byte hexToByte(String hex) {
    return (byte) ((Character.digit(hex.charAt(0), 16) << 4) + Character.digit(hex.charAt(1), 16));
  }
  /** Convert a hex string to a byte array */
  public static byte[] hexToBytes(String hex) {
    int len = hex.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2)
      data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4) + Character.digit(hex.charAt(i + 1), 16));
    return data;
  }

  /** Apply MD5 to the given byte arrays and then make the result a hex-string in bytes */
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

  /** Non-regex form of {@link String#split(String)} */
  public static List<String> splitByChar(String str, char chr) {
    List<String> ret = new ArrayList<>();
    int index = 0;
    while (true) {
      int newIndex = str.indexOf(chr, index);
      if (newIndex == -1) {
        ret.add(str.substring(index));
        break;
      }
      ret.add(str.substring(index, newIndex));
      index = newIndex + 1;
    }
    return ret;
  }

  /** Given a primitive class, get the boxed version */
  public static Class boxedClassFromPrimitive(Class primitive) {
    if (primitive == Void.TYPE) return Void.class;
    if (primitive == Boolean.TYPE) return Boolean.class;
    if (primitive == Byte.TYPE) return Byte.class;
    if (primitive == Character.TYPE) return Character.class;
    if (primitive == Short.TYPE) return Short.class;
    if (primitive == Integer.TYPE) return Integer.class;
    if (primitive == Long.TYPE) return Long.class;
    if (primitive == Float.TYPE) return Float.class;
    if (primitive == Double.TYPE) return Double.class;
    throw new IllegalArgumentException("Unrecognized primitive class: " + primitive);
  }

  /** Given an array component type, return an array type of it */
  public static Class arrayClassOf(Class componentType) {
    if (componentType == boolean.class) return boolean[].class;
    if (componentType == byte.class) return byte[].class;
    if (componentType == char.class) return char[].class;
    if (componentType == double.class) return double[].class;
    if (componentType == float.class) return float[].class;
    if (componentType == int.class) return int[].class;
    if (componentType == long.class) return long[].class;
    if (componentType == short.class) return short[].class;
    ClassLoader cl = componentType.getClassLoader();
    String name;
    if (componentType.isArray()) name = "[" + componentType.getName();
    else name = "[L" + componentType.getName() + ";";
    try {
      return cl != null ? cl.loadClass(name) : Class.forName(name);
    } catch (ClassNotFoundException e) { throw new RuntimeException(e); }
  }

  /** Convert a list to an array, but supports primitive array types */
  @SuppressWarnings("unchecked")
  public static Object listToArray(List list, Class componentType) {
    Object ret = Array.newInstance(componentType, list.size());
    if (componentType == Boolean.TYPE)
      for (int i = 0; i < list.size(); i++) Array.setBoolean(ret, i, (boolean) list.get(i));
    else if (componentType == Byte.TYPE)
      for (int i = 0; i < list.size(); i++) Array.setByte(ret, i, (byte) list.get(i));
    else if (componentType == Character.TYPE)
      for (int i = 0; i < list.size(); i++) Array.setChar(ret, i, (char) list.get(i));
    else if (componentType == Double.TYPE)
      for (int i = 0; i < list.size(); i++) Array.setDouble(ret, i, (double) list.get(i));
    else if (componentType == Float.TYPE)
      for (int i = 0; i < list.size(); i++) Array.setFloat(ret, i, (float) list.get(i));
    else if (componentType == Integer.TYPE)
      for (int i = 0; i < list.size(); i++) Array.setInt(ret, i, (int) list.get(i));
    else if (componentType == Long.TYPE)
      for (int i = 0; i < list.size(); i++) Array.setLong(ret, i, (long) list.get(i));
    else if (componentType == Short.TYPE)
      for (int i = 0; i < list.size(); i++) Array.setShort(ret, i, (short) list.get(i));
    else
      for (int i = 0; i < list.size(); i++) Array.set(ret, i, list.get(i));
    return ret;
  }

  private Util() { }
}
