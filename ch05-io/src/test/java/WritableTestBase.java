// == WritableTestBase
// == WritableTestBase-Deserialize
import java.io.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

/**
 * WritableTestBase - Writable 测试的基类
 *
 * <p>提供序列化和反序列化的辅助方法,
 * 供所有 Writable 测试类使用。</p>
 *
 * <p><b>提供的方法:</b></p>
 * <ul>
 *   <li>serialize(): 将 Writable 序列化为字节数组</li>
 *   <li>deserialize(): 从字节数组反序列化到 Writable</li>
 *   <li>serializeToString(): 序列化为十六进制字符串</li>
 *   <li>writeTo(): 序列化再反序列化,返回十六进制字符串</li>
 * </ul>
 *
 * @see Writable
 */
public class WritableTestBase {

    /**
     * serialize - 将 Writable 序列化为字节数组
     *
     * <p><b>实现:</b></p>
     * <ol>
     *   <li>创建 ByteArrayOutputStream</li>
     *   <li>创建 DataOutputStream 包装</li>
     *   <li>调用 writable.write(dataOut)</li>
     *   <li>返回字节数组</li>
     * </ol>
     *
     * @param writable 要序列化的 Writable 对象
     * @return 序列化后的字节数组
     * @throws IOException 如果发生 I/O 错误
     */
    // vv WritableTestBase
    public static byte[] serialize(Writable writable) throws IOException {
        // 创建内存字节数组输出流
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // 创建数据输出流
        DataOutputStream dataOut = new DataOutputStream(out);
        // 调用 Writable 的序列化方法
        writable.write(dataOut);
        dataOut.close();
        return out.toByteArray();
    }
    // ^^ WritableTestBase

    /**
     * deserialize - 从字节数组反序列化到 Writable
     *
     * @param writable 要填充的 Writable 对象
     * @param bytes 字节数组
     * @return 输入的字节数组 (便于链式调用)
     * @throws IOException 如果发生 I/O 错误
     */
    // vv WritableTestBase-Deserialize
    public static byte[] deserialize(Writable writable, byte[] bytes)
        throws IOException {
        // 创建内存字节数组输入流
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        // 创建数据输入流
        DataInputStream dataIn = new DataInputStream(in);
        // 调用 Writable 的反序列化方法
        writable.readFields(dataIn);
        dataIn.close();
        return bytes;
    }
    // ^^ WritableTestBase-Deserialize

    /**
     * serializeToString - 序列化为十六进制字符串
     *
     * @param src 要序列化的 Writable
     * @return 十六进制字符串
     * @throws IOException 如果发生 I/O 错误
     */
    public static String serializeToString(Writable src) throws IOException {
        return StringUtils.byteToHexString(serialize(src));
    }

    /**
     * writeTo - 序列化再反序列化,返回十六进制字符串
     *
     * <p>常用于验证序列化/反序列化往返的正确性。</p>
     *
     * @param src 源 Writable
     * @param dest 目标 Writable
     * @return 十六进制字符串
     * @throws IOException 如果发生 I/O 错误
     */
    public static String writeTo(Writable src, Writable dest) throws IOException {
        byte[] data = deserialize(dest, serialize(src));
        return StringUtils.byteToHexString(data);
    }

}
