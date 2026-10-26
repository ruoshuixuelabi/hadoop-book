// == WritableTestBase
// == WritableTestBase-Deserialize
import java.io.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

/**
 * 各类 Writable 测试的公共父类
 *
 * <p>提供三个工具方法：
 * 把对象序列化成字节数组、把字节数组反序列化回对象、
 * 以及把序列化结果转成十六进制串便于断言
 * <p>之所以把字节级断言集中放在父类，
 * 是为了让各个子类测试只关心「值对不对」，
 * 而把「字节长什么样」这件事统一到一处
 */
public class WritableTestBase {
  
  // vv WritableTestBase
  /**
   * 把一个 Writable 序列化成字节数组
   *
   * @param writable 待序列化的对象
   * @return 序列化得到的字节
   * @throws IOException 序列化失败
   */
  public static byte[] serialize(Writable writable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    // write 方法由各个 Writable 自己实现序列化格式
    writable.write(dataOut);
    dataOut.close();
    // 内存流的内容可以直接取成字节数组
    return out.toByteArray();
  }
  // ^^ WritableTestBase
  
  // vv WritableTestBase-Deserialize
  /**
   * 把字节数组反序列化进一个已有的 Writable 对象
   *
   * @param writable 接收数据的对象（调用前通常是个空实例）
   * @param bytes    待反序列化的字节
   * @return 原样的 bytes，方便链式调用
   * @throws IOException 反序列化失败
   */
  public static byte[] deserialize(Writable writable, byte[] bytes)
      throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    DataInputStream dataIn = new DataInputStream(in);
    // readFields 会把字段逐个读进对象里，覆盖原有值
    writable.readFields(dataIn);
    dataIn.close();
    return bytes;
  }
  // ^^ WritableTestBase-Deserialize
  
  /**
   * 把对象序列化成十六进制串
   *
   * <p>十六进制形式最适合写进断言：
   * 字节的具体取值一眼就能看出来
   *
   * @param src 待序列化的对象
   * @return 十六进制字符串
   * @throws IOException 序列化失败
   */
  public static String serializeToString(Writable src) throws IOException {
    return StringUtils.byteToHexString(serialize(src));
  }
  
  /**
   * 把 src 序列化后反序列化进 dest，并返回序列化结果的十六进制串
   *
   * <p>一步完成「写出 + 读回」，
   * 既能验证往返是否正确，也能顺带检查字节形式
   *
   * @param src 源对象
   * @param dest 接收数据的对象
   * @return 序列化结果的十六进制串
   * @throws IOException 序列化或反序列化失败
   */
  public static String writeTo(Writable src, Writable dest) throws IOException {
    // 先序列化再反序列化，dest 里应当得到与 src 等价的内容
    byte[] data = deserialize(dest, serialize(src));
    return StringUtils.byteToHexString(data);
  }

}
