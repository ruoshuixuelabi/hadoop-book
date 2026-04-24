import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * FileDecompressorTest - FileDecompressor 的单元测试
 *
 * <p>测试 FileDecompressor 对 GZIP 文件的解压缩功能。</p>
 *
 * <p><b>测试流程:</b></p>
 * <ol>
 *   <li>创建临时 GZIP 文件</li>
 *   <li>调用 FileDecompressor.main() 进行解压缩</li>
 *   <li>验证解压缩后的内容</li>
 * </ol>
 *
 * @see FileDecompressor
 */
public class FileDecompressorTest {

    /**
     * decompressesGzippedFile - 测试 GZIP 文件解压缩
     *
     * <p><b>测试步骤:</b></p>
     * <ol>
     *   <li>从 classpath 读取测试资源 /file.gz</li>
     *   <li>复制到临时文件</li>
     *   <li>调用 FileDecompressor 解压缩</li>
     *   <li>验证解压缩后的内容为 "Text\n"</li>
     * </ol>
     *
     * @throws Exception 如果测试失败
     */
    @Test
    public void decompressesGzippedFile() throws Exception {
        // 创建临时文件,扩展名为 .gz
        File file = File.createTempFile("file", ".gz");
        // JVM 退出时删除临时文件
        file.deleteOnExit();

        // 从 classpath 读取测试资源
        InputStream in = this.getClass().getResourceAsStream("/file.gz");
        // 复制到临时文件
        IOUtils.copyBytes(in, new FileOutputStream(file), 4096, true);

        // 获取临时文件的绝对路径
        String path = file.getAbsolutePath();

        // 调用 FileDecompressor 解压缩
        FileDecompressor.main(new String[] { path });

        // 计算解压缩后的文件路径 (移除 .gz 后缀)
        String decompressedPath = path.substring(0, path.length() - 3);

        // 验证解压缩后的内容
        assertThat(readFile(new File(decompressedPath)), is("Text\n"));
    }

    /**
     * readFile - 读取文件内容
     *
     * @param file 要读取的文件
     * @return 文件内容字符串
     * @throws IOException 如果读取失败
     */
    private String readFile(File file) throws IOException {
        // 使用 Scanner 读取整个文件
        // useDelimiter("\\A") 表示读取整个输入
        return new Scanner(file).useDelimiter("\\A").next();
    }
}
