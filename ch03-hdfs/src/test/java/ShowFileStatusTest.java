// cc ShowFileStatusTest Demonstrates file status information
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;

// vv ShowFileStatusTest
/**
 * 验证 {@link FileStatus} 元数据内容的测试
 *
 * <p>本测试会真实启动一个进程内的 HDFS 集群（{@link MiniDFSCluster}），
 * 因此拿到的是与生产环境一致的元数据：权限位、属主属组、副本数、块大小等
 * <p>需要能创建符号链接或在临时目录写入的环境
 */
public class ShowFileStatusTest {
  
  /** 进程内 HDFS 集群实例，用于测试 */
  private MiniDFSCluster cluster; // use an in-process HDFS cluster for testing
  /** 从测试集群取得的文件系统客户端 */
  private FileSystem fs;

  /**
   * 启动测试用 HDFS 集群并准备测试文件
   *
   * @throws IOException 集群启动或文件创建失败
   */
  @Before
  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    // 允许外部覆盖测试数据目录，便于在 CI 中指定位置
    if (System.getProperty("test.build.data") == null) {
      System.setProperty("test.build.data", "/tmp");
    }
    // 在当前 JVM 内启动一个完整的 HDFS：NameNode + DataNode
    cluster = new MiniDFSCluster.Builder(conf).build();
    fs = cluster.getFileSystem();
    OutputStream out = fs.create(new Path("/dir/file"));
    out.write("content".getBytes("UTF-8"));
    out.close();
  }
  
  /**
   * 关闭文件系统客户端并停止测试集群
   *
   * @throws IOException 关闭过程出错
   */
  @After
  public void tearDown() throws IOException {
    if (fs != null) { fs.close(); }
    if (cluster != null) { cluster.shutdown(); }
  }
  
  /**
   * 验证查询不存在的文件会抛出 FileNotFoundException
   *
   * @throws IOException 本用例预期由 JUnit 捕获
   */
  @Test(expected = FileNotFoundException.class)
  public void throwsFileNotFoundForNonExistentFile() throws IOException {
    fs.getFileStatus(new Path("no-such-file"));
  }
  
  /**
   * 验证普通文件的元数据
   *
   * @throws IOException 查询元数据失败
   */
  @Test
  public void fileStatusForFile() throws IOException {
    Path file = new Path("/dir/file");
    FileStatus stat = fs.getFileStatus(file);
    assertThat(stat.getPath().toUri().getPath(), is("/dir/file"));
    assertThat(stat.isDirectory(), is(false));
    // 长度对应写入的 "content" 共 7 个字节
    assertThat(stat.getLen(), is(7L));
    // 修改时间不应晚于当前时刻
    assertThat(stat.getModificationTime(),
        is(lessThanOrEqualTo(System.currentTimeMillis())));
    // 默认副本数为 1
    assertThat(stat.getReplication(), is((short) 1));
    // 默认块大小为 128MB
    assertThat(stat.getBlockSize(), is(128 * 1024 * 1024L));
    // 属主为运行测试的操作系统用户
    assertThat(stat.getOwner(), is(System.getProperty("user.name")));
    // HDFS 用超级用户组统一管理权限
    assertThat(stat.getGroup(), is("supergroup"));
    // 默认权限：属主可读写，其他人只读
    assertThat(stat.getPermission().toString(), is("rw-r--r--"));
  }
  
  /**
   * 验证目录的元数据
   *
   * <p>与文件相比，目录的副本数与块大小均为 0，权限多一位可执行位
   *
   * @throws IOException 查询元数据失败
   */
  @Test
  public void fileStatusForDirectory() throws IOException {
    Path dir = new Path("/dir");
    FileStatus stat = fs.getFileStatus(dir);
    assertThat(stat.getPath().toUri().getPath(), is("/dir"));
    assertThat(stat.isDirectory(), is(true));
    // 目录本身没有内容，长度为 0
    assertThat(stat.getLen(), is(0L));
    assertThat(stat.getModificationTime(),
        is(lessThanOrEqualTo(System.currentTimeMillis())));
    // 目录不参与副本复制
    assertThat(stat.getReplication(), is((short) 0));
    // 目录没有数据块
    assertThat(stat.getBlockSize(), is(0L));
    assertThat(stat.getOwner(), is(System.getProperty("user.name")));
    assertThat(stat.getGroup(), is("supergroup"));
    // 目录需要可执行位才能被进入
    assertThat(stat.getPermission().toString(), is("rwxr-xr-x"));
  }
  
}
// ^^ ShowFileStatusTest
