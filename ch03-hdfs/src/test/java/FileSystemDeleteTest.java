import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * 验证 HDFS 文件与目录删除语义的测试
 *
 * <p>重点验证 {@link FileSystem#delete} 的第二个参数（是否递归）：
 * 非递归删除非空目录必须失败并抛异常，递归删除则应连同内容一并清除
 */
public class FileSystemDeleteTest {

  /** 底层文件系统实例 */
  private FileSystem fs;
  
  /**
   * 在每个用例前创建测试文件
   *
   * <p>建出 "dir/file"，使 "dir" 成为一个非空目录，
   * 这样才能验证非递归删除非空目录会失败
   *
   * @throws Exception 文件系统操作失败
   */
  @Before
  public void setUp() throws Exception {
    fs = FileSystem.get(new Configuration());
    writeFile(fs, new Path("dir/file"));
  }
  
  /**
   * 创建一个空文件
   *
   * @param fileSys 目标文件系统
   * @param name    文件路径
   * @throws IOException 创建文件失败
   */
  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    // create 返回输出流；这里只需文件存在，故创建后立即关闭
    FSDataOutputStream stm = fileSys.create(name);
    stm.close();
  }
  
  /**
   * 验证逐级删除文件与空目录
   *
   * @throws Exception 文件系统操作失败
   */
  @Test
  public void deleteFile() throws Exception {
    // 非递归删除单个文件成功
    assertThat(fs.delete(new Path("dir/file"), false), is(true));
    assertThat(fs.exists(new Path("dir/file")), is(false));
    // 文件删除后目录仍存在，因为删除文件不影响父目录
    assertThat(fs.exists(new Path("dir")), is(true));
    // 此时目录已空，非递归删除同样成功
    assertThat(fs.delete(new Path("dir"), false), is(true));
    assertThat(fs.exists(new Path("dir")), is(false));
  }

  /**
   * 验证非递归删除非空目录会失败
   *
   * @throws Exception 文件系统操作失败
   */
  @Test
  public void deleteNonEmptyDirectoryNonRecursivelyFails() throws Exception {
    try {
      // 目录中还有 file，递归参数为 false 时应抛 IOException
      fs.delete(new Path("dir"), false);
      // 若走到这里说明删除意外成功了，用 fail 让测试失败
      fail("Shouldn't delete non-empty directory");
    } catch (IOException e) {
      // expected
    }
  }
  
  /**
   * 验证递归删除目录会连同内容一起清除
   *
   * @throws Exception 文件系统操作失败
   */
  @Test
  public void deleteDirectory() throws Exception {
    // 递归参数为 true，允许删除非空目录
    assertThat(fs.delete(new Path("dir"), true), is(true));
    assertThat(fs.exists(new Path("dir")), is(false));
  }
  
}
