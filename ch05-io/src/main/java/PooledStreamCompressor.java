// cc PooledStreamCompressor A program to compress data read from standard input and write it to standard output using a pooled compressor
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

// vv PooledStreamCompressor
/**
 * 从标准输入读数据、压缩后写到标准输出的演示程序（使用编解码器池）
 *
 * <p>{@link org.apache.hadoop.io.compress.CompressionCodecPool} 用来复用
 * {@link org.apache.hadoop.io.compress.Compressor} 实例。
 * 每个 Compressor 内部都有一块缓冲区，为每个流新建一个代价很高；
 * 从池里借一个、用完还回去，就能反复复用同一块内存
 * <p>适用于要连续压缩很多条流、且每条流都不大的场景
 */
public class PooledStreamCompressor {

  /**
   * 把标准输入压缩后写到标准输出，压缩器从池中借用
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
    // compressor 要在 try 之外声明，finally 里才能归还给池
    /*[*/Compressor compressor = null;
    // try 负责借出压缩器，finally 负责归还
    try {
      // 从池里借一个 Compressor；池空时会新建一个
      compressor = CodecPool.getCompressor(codec);/*]*/
      // 把借来的 Compressor 交给输出流使用，缓冲区就能被复用
      CompressionOutputStream out =
        codec.createOutputStream(System.out, /*[*/compressor/*]*/);
      // 缓冲区 4096 字节；最后一个参数 false 表示不关闭底层流
      IOUtils.copyBytes(System.in, out, 4096, false);
      // finish 会把压缩缓冲里的剩余数据刷出去并写完尾部标记；
      // 不调用 finish 的话，压缩流不会被正确收尾，输出无法解压
      out.finish();
    // 无论正常结束还是抛异常，压缩器都必须归还给池
    /*[*/} finally {
      // 归还压缩器，池子才能在下一个流上复用它
      CodecPool.returnCompressor(compressor);
    }/*]*/
  }
}
// ^^ PooledStreamCompressor
