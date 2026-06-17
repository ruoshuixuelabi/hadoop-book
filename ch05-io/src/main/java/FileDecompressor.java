// cc FileDecompressor A program to decompress a compressed file using a codec inferred from the file's extension
// cc FileDecompressorDecompressor 解压缩程序,根据文件扩展名自动推断压缩编解码器

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
 * FileDecompressor - 文件解压缩程序
 *
 * <p>此程序演示如何使用 Hadoop 的 CompressionCodecFactory 自动检测文件格式并解压缩。
 * 它根据文件扩展名推断应该使用哪种压缩编解码器,然后读取并解压缩文件。</p>
 *
 * <p><b>支持的压缩格式:</b></p>
 * <ul>
 *   <li>.gz - GZIP 压缩 (使用 GzipCodec)</li>
 *   <li>.bz2 - BZip2 压缩 (使用 BZip2Codec)</li>
 *   <li>.lz4 - LZ4 压缩 (使用 Lz4Codec)</li>
 *   <li>.snappy - Snappy 压缩 (使用 SnappyCodec)</li>
 *   <li>.deflate - Deflate 压缩 (使用 DeflateCodec)</li>
 *   <li>以及其他 Hadoop 支持的压缩格式</li>
 * </ul>
 *
 * <p><b>工作原理:</b></p>
 * <ol>
 *   <li>创建 CompressionCodecFactory 并传入 Configuration</li>
 *   <li>调用 factory.getCodec(path) 根据文件扩展名获取对应的 Codec</li>
 *   <li>使用 codec.createInputStream() 创建解压缩输入流</li>
 *   <li>使用 IOUtils.copyBytes 复制数据</li>
 *   <li>自动移除压缩文件的后缀名作为输出文件名</li>
 * </ol>
 *
 * <p><b>使用示例:</b></p>
 * <pre>{@code
 * # 解压缩文件.txt.gz
 * hadoop jar hadoop-examples.jar FileDecompressor /path/to/file.txt.gz
 * # 输出: file.txt (原始文件被恢复)
 * }</pre>
 *
 * @see CompressionCodecFactory
 * @see CompressionCodec
 * @see StreamCompressor (流压缩版本)
 */
public class FileDecompressor {

    /**
     * 主方法 - 解压缩文件
     *
     * <p><b>参数:</b></p>
     * <ul>
     *   <li>args[0]: 要解压缩的文件路径 (HDFS 或本地文件系统路径)</li>
     * </ul>
     *
     * <p><b>处理流程:</b></p>
     * <ol>
     *   <li>解析命令行参数,获取文件 URI</li>
     *   <li>获取 FileSystem 实例</li>
     *   <li>创建 CompressionCodecFactory</li>
     *   <li>根据文件扩展名获取对应的 CompressionCodec</li>
     *   <li>创建解压缩输入流</li>
     *   <li>创建输出流</li>
     *   <li>复制数据(自动解压缩)</li>
     *   <li>关闭所有流</li>
     * </ol>
     *
     * <p><b>错误处理:</b></p>
     * <ul>
     *   <li>如果找不到对应扩展名的 Codec,打印错误信息并退出</li>
     *   <li>使用 finally 确保流被正确关闭</li>
     * </ul>
     *
     * @param args 命令行参数
     * @throws Exception 如果发生错误
     */
    public static void main(String[] args) throws Exception {
        // 获取要解压缩的文件路径
        String uri = args[0];

        // 创建 Hadoop Configuration 对象
        // Configuration 会加载 classpath 中的 hadoop-default.xml 和 hadoop-site.xml
        Configuration conf = new Configuration();

        // 获取对应的 FileSystem
        // 这会根据 URI 的 scheme (hdfs://, file://, etc.) 返回相应的文件系统实现
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        // 创建 Path 对象
        Path inputPath = new Path(uri);

        // 创建 CompressionCodecFactory
        // CodecFactory 会扫描 classpath 中所有注册的 CompressionCodec
        // 并根据文件扩展名建立映射关系
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);

        // 根据文件路径获取对应的 CompressionCodec
        // 如果文件扩展名没有对应的 Codec,返回 null
        CompressionCodec codec = factory.getCodec(inputPath);

        // 如果找不到 Codec,打印错误并退出
        if (codec == null) {
            System.err.println("No codec found for " + uri);
            System.exit(1);
        }

        // 根据 Codec 的默认扩展名移除压缩文件的后缀
        // 例如: file.txt.gz -> file.txt
        String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());

        // 解压缩输入流和输出流
        InputStream in = null;
        OutputStream out = null;
        try {
            // 创建解压缩输入流
            // codec.createInputStream(fs.open(inputPath))
            // 会自动包装底层的文件输入流,实现透明的解压缩
            in = codec.createInputStream(fs.open(inputPath));

            // 创建输出流,目标路径是移除压缩后缀后的路径
            out = fs.create(new Path(outputUri));

            // 使用 IOUtils.copyBytes 复制数据
            // 参数: InputStream, OutputStream, bufferSize, closeStreams
            // 内部会处理解压缩逻辑
            IOUtils.copyBytes(in, out, conf);
        }
        finally {
            // 关闭输入流
            IOUtils.closeStream(in);
            // 关闭输出流
            IOUtils.closeStream(out);
        }
    }
}
// ^^ FileDecompressor
