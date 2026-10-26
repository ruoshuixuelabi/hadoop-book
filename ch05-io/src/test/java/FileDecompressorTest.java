import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * 验证 {@link FileDecompressor} 能按扩展名推断格式并正确解压
 *
 * <p>{@code FileDecompressor} 靠文件名后缀决定用哪种
 * {@link org.apache.hadoop.io.compress.CompressionCodec}，
 * 所以本测试先造一个 {@code .gz} 文件，
 * 再跑一遍解压程序，检查产物内容
 */
public class FileDecompressorTest {

  /**
   * 造一个 gzip 文件、解压它，再校验解压结果的内容
   *
   * @throws Exception 建文件或解压过程出错
   */
  @Test
  public void decompressesGzippedFile() throws Exception {
    // 临时文件名以 .gz 结尾，解压程序才能推断出要用 GzipCodec
    File file = File.createTempFile("file", ".gz");
    file.deleteOnExit();
    // 样本文件放在测试资源目录里，跟着 classpath 走
    InputStream in = this.getClass().getResourceAsStream("/file.gz");
    // 最后一个参数 true 表示复制完顺便关掉目标流
    IOUtils.copyBytes(in, new FileOutputStream(file), 4096, true);
    
    String path = file.getAbsolutePath();
    // 直接调用被测程序的 main，与命令行运行的效果一致
    FileDecompressor.main(new String[] { path });
    
    // 去掉末尾的 ".gz" 三字符，就是解压产物的路径
    String decompressedPath = path.substring(0, path.length() - 3);
    assertThat(readFile(new File(decompressedPath)), is("Text\n"));
  }
  
  /**
   * 把整个文件读成一个字符串
   *
   * @param file 待读取的文件
   * @return 文件的完整内容
   * @throws IOException 读取失败
   */
  private String readFile(File file) throws IOException {
    // 界限定为 \A（输入开头），这样连换行在内的全文都能读到
    return new Scanner(file).useDelimiter("\\A").next();
  }
}
