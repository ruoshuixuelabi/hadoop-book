import static org.hamcrest.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * HDFS文件删除操作测试类。
 *
 * <p>此类测试FileSystem.delete()方法的各种场景，包括：</p>
 * <ul>
 *   <li>删除空文件</li>
 *   <li>递归删除非空目录</li>
 *   <li>非递归删除非空目录应失败</li>
 *   <li>删除目录（递归）</li>
 * </ul>
 *
 * <p>HDFS删除特点：</p>
 * <ul>
 *   <li>删除文件/目录后，空间不会立即释放（HDFS垃圾箱机制）</li>
 *   <li>非空目录非递归删除会抛出IOException</li>
 *   <li>递归删除会删除目录及其所有内容</li>
 * </ul>
 *
 * @see FileSystem#delete(Path, boolean) 删除方法
 */
public class FileSystemDeleteTest {

  // 测试使用的FileSystem实例
  private FileSystem fs;

  /**
   * 测试前准备：创建FileSystem实例和测试文件。
   *
   * <p>创建一个测试目录dir和文件dir/file用于后续删除测试。</p>
   *
   * @throws Exception 如果准备失败
   */
  @BeforeEach
  public void setUp() throws Exception {
    // 获取默认的FileSystem实例（通常是HDFS）
    fs = FileSystem.get(new Configuration());
    // 创建测试文件 dir/file
    writeFile(fs, new Path("dir/file"));
  }

  /**
   * 辅助方法：创建空文件。
   *
   * <p>用于测试准备，创建指定路径的空文件。</p>
   *
   * @param fileSys 文件系统实例
   * @param name 文件路径
   * @throws IOException 如果创建失败
   */
  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    // 创建文件并立即关闭
    FSDataOutputStream stm = fileSys.create(name);
    stm.close();
  }

  /**
   * 测试：删除空文件。
   *
   * <p>验证删除文件后：</p>
   * <ul>
   *   <li>delete()返回true</li>
   *   <li>文件不再存在</li>
   * </ul>
   *
   * @throws Exception 如果测试失败
   */
  @Test
  public void deleteFile() throws Exception {
    // 删除文件，第二个参数false表示非递归（对文件无影响）
    assertThat(fs.delete(new Path("dir/file"), false), is(true));
    // 验证文件不存在
    assertThat(fs.exists(new Path("dir/file")), is(false));
    // 验证父目录仍然存在
    assertThat(fs.exists(new Path("dir")), is(true));
    // 删除父目录（非递归会失败，如果不为空）
    assertThat(fs.delete(new Path("dir"), false), is(true));
    // 验证目录不存在
    assertThat(fs.exists(new Path("dir")), is(false));
  }

  /**
   * 测试：非递归删除非空目录应失败。
   *
   * <p>当目录不为空且第二个参数为false时，delete()应抛出IOException。
   * 这是HDFS的安全机制，防止误删包含文件的目录。</p>
   *
   * @throws Exception 期望抛出IOException
   */
  @Test
  public void deleteNonEmptyDirectoryNonRecursivelyFails() throws Exception {
    try {
      // 尝试非递归删除非空目录，应失败
      fs.delete(new Path("dir"), false);
      // 如果没有抛出异常，测试失败
      fail("Shouldn't delete non-empty directory");
    } catch (IOException e) {
      // 期望的异常行为：非空目录非递归删除会抛出IOException
      // 测试通过
    }
  }

  /**
   * 测试：递归删除非空目录。
   *
   * <p>当第二个参数为true时，即使目录非空也会被删除。
   * 删除操作会递归删除目录及其所有内容。</p>
   *
   * @throws Exception 如果测试失败
   */
  @Test
  public void deleteDirectory() throws Exception {
    // 递归删除非空目录
    assertThat(fs.delete(new Path("dir"), true), is(true));
    // 验证目录不存在（递归删除成功）
    assertThat(fs.exists(new Path("dir")), is(false));
  }

}
