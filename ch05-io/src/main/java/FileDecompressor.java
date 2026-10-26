// cc FileDecompressor A program to decompress a compressed file using a codec inferred from the file's extension
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

// vv FileDecompressor
/**
 * 按文件扩展名推断压缩格式并解压的演示程序
 *
 * <p>{@link org.apache.hadoop.io.compress.CompressionCodecFactory}
 * 能根据文件名的扩展名（{@code .gz}、{@code .bz2} 等）
 * 判断出该用哪个 {@link org.apache.hadoop.io.compress.CompressionCodec}，
 * 前提是 Hadoop 启用了相应的压缩编解码器
 */
public class FileDecompressor {

  /**
   * 解压指定文件，输出文件名为去掉压缩扩展名后的名字
   *
   * @param args 第一个参数是待解压文件的 URI
   * @throws Exception 文件系统操作失败
   */
  public static void main(String[] args) throws Exception {
    // 待解压文件的 URI
    String uri = args[0];
    // Configuration 汇总了 core-site.xml、hdfs-site.xml 等配置文件里的设置
    Configuration conf = new Configuration();
    // FileSystem.get 会按 URI 的 scheme 缓存实例，重复调用不会重复建立连接
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    
    // 输入路径，编解码器就是按这个路径的扩展名来推断的
    Path inputPath = new Path(uri);
    // 工厂对象负责按扩展名挑出对应的编解码器
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    // 按扩展名推断该用哪种编解码器
    CompressionCodec codec = factory.getCodec(inputPath);
    // 找不到对应编解码器就退出，避免后面用 null 造成难以理解的异常
    if (codec == null) {
      System.err.println("No codec found for " + uri);
      System.exit(1);
    }

    // 去掉默认扩展名（如 .gz）作为输出文件名
    String outputUri =
      CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());

    // 输入流要在 try 之外声明，finally 里才能关闭
    InputStream in = null;
    // 输出流同理
    OutputStream out = null;
    // try 负责打开流，finally 负责关闭
    try {
      // 由编解码器包一层，得到能自动解压的输入流
      in = codec.createInputStream(fs.open(inputPath));
      // 创建解压后的目标文件
      out = fs.create(new Path(outputUri));
      // 按缓冲区大小分块复制，避免一次性把大文件读进内存
      IOUtils.copyBytes(in, out, conf);
    // finally 保证两个流一定会被关闭
    } finally {
      // 输入流要关，closeStream 内部已做 null 判断
      IOUtils.closeStream(in);
      // 输出流也要关，否则数据可能还没落盘
      IOUtils.closeStream(out);
    }
  }
}
// ^^ FileDecompressor
