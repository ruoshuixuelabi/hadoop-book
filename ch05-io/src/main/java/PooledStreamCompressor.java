// cc PooledStreamCompressor 使用对象池压缩标准输入数据到标准输出的程序

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

// vv PooledStreamCompressor
/**
 * PooledStreamCompressor - 使用 CodecPool 的流压缩器
 *
 * <p>此程序是 StreamCompressor 的增强版本,使用 CodecPool 管理 Compressor 对象池。
 * 对象池可以重用 Compressor 对象,减少频繁创建/销毁对象的开销。</p>
 *
 * <p><b>对象池优势:</b></p>
 * <ul>
 *   <li><b>性能提升</b>: Compressor 创建成本高,池化可复用</li>
 *   <li><b>资源管理</b>: 避免创建过多 Compressor 导致内存压力</li>
 *   <li><b>线程安全</b>: CodecPool 管理 Compressor 的获取和归还</li>
 * </ul>
 *
 * <p><b>CodecPool 工作机制:</b></p>
 * <ul>
 *   <li>getCompressor(codec): 从池中获取 Compressor,池空时创建新的</li>
 *   <li>returnCompressor(compressor): 将 Compressor 返回池中</li>
 * </ul>
 *
 * <p><b>使用示例:</b></p>
 * <pre>{@code
 * cat data.txt | hadoop PooledStreamCompressor org.apache.hadoop.io.compress.SnappyCodec > data.snappy
 * }</pre>
 *
 * <p><b>注意:</b></p>
 * <ul>
 *   <li>使用完必须调用 returnCompressor 归还对象</li>
 *   <li>应在 finally 块中确保归还,避免资源泄漏</li>
 * </ul>
 *
 * @see StreamCompressor
 * @see CodecPool
 * @see Compressor
 */
public class PooledStreamCompressor {

    /**
     * 主方法 - 使用对象池进行流压缩
     *
     * <p><b>命令行参数:</b></p>
     * <ul>
     *   <li>args[0]: CompressionCodec 的完全限定类名</li>
     * </ul>
     *
     * <p><b>处理流程:</b></p>
     * <ol>
     *   <li>从命令行加载 CompressionCodec 类</li>
     *   <li>从 CodecPool 获取 Compressor</li>
     *   <li>创建 CompressionOutputStream 并绑定 Compressor</li>
     *   <li>复制数据并进行压缩</li>
     *   <li>将 Compressor 返回池中</li>
     * </ol>
     *
     * @param args 命令行参数
     * @throws Exception 如果发生错误
     */
    public static void main(String[] args) throws Exception {
        String codecClassname = args[0];
        Class<?> codecClass = Class.forName(codecClassname);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec)
                ReflectionUtils.newInstance(codecClass, conf);

        /*[*/
        // 从对象池获取 Compressor
        // CodecPool 内部维护一个 Map<CompressionCodec, LinkedList<Compressor>>
        // 如果池中有可用的 Compressor,直接返回;否则创建新的
        Compressor compressor = null;
        try {
            // 从池中获取 Compressor
            compressor = CodecPool.getCompressor(codec);/*]*/

            // 创建压缩输出流,第二个参数传入 Compressor 以复用
            // 不传 Compressor 时,codec.createOutputStream(System.out) 会创建临时 Compressor
            CompressionOutputStream out =
                    codec.createOutputStream(System.out, /*[*/compressor/*]*/);

            // 复制并压缩数据
            IOUtils.copyBytes(System.in, out, 4096, false);

            // finish() 完成压缩并刷新缓冲区
            out.finish();

            /*[*/
        }
        finally {
            // 将 Compressor 返回对象池
            // 这是关键步骤:确保资源被正确归还,避免内存泄漏
            CodecPool.returnCompressor(compressor);
        }/*]*/
    }
}
// ^^ PooledStreamCompressor
