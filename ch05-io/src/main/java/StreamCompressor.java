// cc StreamCompressor A program to compress data read from standard input and write it to standard output
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

// vv StreamCompressor
/**
 * 从标准输入读数据、压缩后写到标准输出的演示程序
 *
 * <p>用法：{@code hadoop jar ... StreamCompressor <编解码器类名> < 原始数据 > 压缩数据}
 * <p>编解码器以类名形式传入（比如
 * {@code org.apache.hadoop.io.compress.GzipCodec}），
 * 再由 {@link org.apache.hadoop.util.ReflectionUtils} 反射实例化，
 * 这样新增一种编解码器时无需修改本程序
 */
public class StreamCompressor {

  /**
   * 把标准输入压缩后写到标准输出
   *
   * @param args 第一个参数是压缩编解码器的类名
   * @throws Exception 类加载或流操作失败
   */
  public static void main(String[] args) throws Exception {
    // 编解码器以类名形式传入，靠反射加载，新增编解码器无需改代码
    String codecClassname = args[0];
    // 用类名反射加载编解码器类
    Class<?> codecClass = Class.forName(codecClassname);
    // 编解码器初始化时需要一份配置
    Configuration conf = new Configuration();
    // 反射创建实例并转型成 CompressionCodec
    CompressionCodec codec = (CompressionCodec)
      ReflectionUtils.newInstance(codecClass, conf);
    
    // 包一层输出流，写入的数据会被自动压缩
    CompressionOutputStream out = codec.createOutputStream(System.out);
    // 缓冲区 4096 字节；最后一个参数 false 表示不关闭底层流
    IOUtils.copyBytes(System.in, out, 4096, false);
    // finish 会把压缩缓冲里的剩余数据刷出去并写完尾部标记；
    // 不调用 finish 的话，压缩流不会被正确收尾，输出无法解压
    out.finish();
  }
}
// ^^ StreamCompressor
