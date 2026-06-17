// cc StreamCompressor 演示如何从标准输入读取数据并压缩后输出到标准输出

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

// vv StreamCompressor
/**
 * StreamCompressor - 基于流的压缩器演示
 *
 * <p>此程序演示如何使用 Hadoop 的 CompressionCodec 对数据流进行压缩。
 * 它从标准输入读取数据,压缩后写入标准输出。</p>
 *
 * <p><b>支持的压缩格式:</b></p>
 * <ul>
 *   <li>GzipCodec: 基于 DEFLATE,高压缩比,兼容性好</li>
 *   <li>BZip2Codec: 基于 BZip2,更高压缩比,支持分片</li>
 *   <li>SnappyCodec: 快速压缩,适合 Spark/MapReduce shuffle</li>
 *   <li>Lz4Codec: 极速压缩,适合本地数据传输</li>
 * </ul>
 *
 * <p><b>使用示例:</b></p>
 * <pre>{@code
 * # 压缩文件
 * cat file.txt | hadoop StreamCompressor org.apache.hadoop.io.compress.GzipCodec > file.txt.gz
 *
 * # 解压缩文件
 * cat file.txt.gz | hadoop StreamCompressor org.apache.hadoop.io.compress.GzipCodec | gunzip
 * }</pre>
 *
 * <p><b>工作流程:</b></p>
 * <ol>
 *   <li>根据命令行参数反射加载 CompressionCodec</li>
 *   <li>使用 Codec 创建 CompressionOutputStream</li>
 *   <li>将 stdin 的数据复制到压缩流</li>
 *   <li>调用 finish() 完成压缩并刷新</li>
 * </ol>
 *
 * @see CompressionCodec
 * @see CompressionOutputStream
 * @see PooledStreamCompressor (带对象池的版本)
 */
public class StreamCompressor {

    /**
     * 主方法 - 创建并运行流压缩器
     *
     * <p><b>命令行参数:</b></p>
     * <ul>
     *   <li>args[0]: CompressionCodec 的完全限定类名</li>
     * </ul>
     *
     * <p><b>输入:</b> 从标准输入读取未压缩的字节数据</p>
     * <p><b>输出:</b> 向标准输出写入压缩后的字节数据</p>
     *
     * @param args 命令行参数
     * @throws Exception 如果压缩过程中发生错误
     */
    public static void main(String[] args) throws Exception {
        // 从命令行获取 Codec 类名
        // 例如: org.apache.hadoop.io.compress.GzipCodec
        String codecClassname = args[0];

        // 使用反射加载 Codec 类
        Class<?> codecClass = Class.forName(codecClassname);

        // 创建 Configuration (Hadoop 配置,默认加载 classpath 中的配置)
        Configuration conf = new Configuration();

        // 使用 ReflectionUtils 创建 CompressionCodec 实例
        // ReflectionUtils.newInstance 会调用 codecClass 的无参构造方法
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        // 创建压缩输出流
        // createOutputStream(System.out) 将压缩数据直接写入标准输出
        // 注意: 这会自动处理压缩格式(添加文件头/尾等)
        CompressionOutputStream out = codec.createOutputStream(System.out);

        // 使用 IOUtils.copyBytes 将 stdin 的内容复制到压缩流
        // 参数: InputStream, OutputStream, bufferSize, closeOutputStream
        // 最后一个参数为 false 表示不关闭底层输出流(System.out)
        IOUtils.copyBytes(System.in, out, 4096, false);

        // finish() 完成压缩并刷新缓冲区
        // 某些 Codec(如 DeflateCodec) 需要调用 finish 才能输出所有数据
        out.finish();
    }
}
// ^^ StreamCompressor
