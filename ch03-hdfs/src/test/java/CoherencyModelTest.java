// == CoherencyModelTest
// == CoherencyModelTest-NotVisibleAfterFlush
// == CoherencyModelTest-VisibleAfterFlushAndSync
// == CoherencyModelTest-LocalFileVisibleAfterFlush
// == CoherencyModelTest-VisibleAfterClose
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;

/**
 * 验证 HDFS 的「一致性模型」：数据在什么时刻对读取者可见
 *
 * <p>HDFS 客户端写数据时，数据先在客户端缓冲区，再经由 DataNode 管道落盘；
 * 各个 API 调用对应的可见性时点不同：
 * <p>{@code flush()} 只把数据交给操作系统，用户态缓冲区清空但内容未必落盘；
 * {@code hflush()} 保证数据已写入 DataNode 的操作系统缓冲区；
 * {@code hsync()} 保证数据已真正落盘到 DataNode 的物理磁盘；
 * {@code close()} 则关闭文件，隐含一次完整同步
 * <p>本测试通过断言 {@code getFileStatus} 返回的长度，
 * 观察每个时点上客户端能否看到已写入的内容
 */
public class CoherencyModelTest {
  /** 进程内 HDFS 集群实例，用于测试 */
  private MiniDFSCluster cluster; // use an in-process HDFS cluster for testing
  /** 从测试集群取得的文件系统客户端 */
  private FileSystem fs;

  /**
   * 启动进程内 HDFS 集群
   *
   * @throws IOException 集群启动失败
   */
  @Before
  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    // 允许外部覆盖测试数据目录，便于在 CI 中指定位置
    if (System.getProperty("test.build.data") == null) {
      System.setProperty("test.build.data", "/tmp");
    }
    // 在当前 JVM 内启动 NameNode 与 DataNode
    cluster = new MiniDFSCluster.Builder(conf).build();
    fs = cluster.getFileSystem();
  }
  
  /**
   * 关闭客户端并停止测试集群
   *
   * @throws IOException 关闭过程出错
   */
  @After
  public void tearDown() throws IOException {
    fs.close();
    cluster.shutdown();
  }
  
  /**
   * 验证文件一旦创建就立即可见
   *
   * <p>文件的存在性与内容长度是两件事：
   * {@code create} 返回后目录项已在 NameNode 上登记，故 {@code exists} 立刻为真；
   * 但此时文件长度仍为 0，因为内容尚未写入
   *
   * @throws IOException 文件系统操作失败
   */
  @Test
  public void fileExistsImmediatelyAfterCreation() throws IOException {
    // vv CoherencyModelTest-ExistsImmediatelyAfterCreation
    Path p = new Path("p");
    fs.create(p);
    assertThat(fs.exists(p), is(true));
    // ^^ CoherencyModelTest-ExistsImmediatelyAfterCreation
    assertThat(fs.delete(p, true), is(true));
  }
  
  /**
   * 验证 {@code flush()} 之后内容对读取者仍不可见
   *
   * <p>{@code flush()} 只把数据推给操作系统，
   * 而 HDFS 客户端要等数据包真正发给 DataNode 后，
   * NameNode 才会更新文件长度，所以此刻长度仍是 0
   *
   * @throws IOException 文件系统操作失败
   */
  @Test
  public void fileContentIsNotVisibleAfterFlush() throws IOException {
    // vv CoherencyModelTest-NotVisibleAfterFlush
    Path p = new Path("p");
    OutputStream out = fs.create(p);
    out.write("content".getBytes("UTF-8"));
    /*[*/out.flush();/*]*/
    assertThat(fs.getFileStatus(p).getLen(), is(0L));
    // ^^ CoherencyModelTest-NotVisibleAfterFlush
    out.close();
    assertThat(fs.delete(p, true), is(true));
  }
  
  /**
   * 验证 {@code hflush()} 之后内容变为可见
   *
   * <p>{@code hflush()} 是 HDFS 特有的调用：
   * 它保证数据已经写入 DataNode 的缓冲区，管道中的包已被确认，
   * 此时 NameNode 已能报告出真实的文件长度
   *
   * @throws IOException 文件系统操作失败
   */
  @Test
  public void fileContentIsVisibleAfterHFlush() throws IOException {
    // vv CoherencyModelTest-VisibleAfterHFlush
    Path p = new Path("p");
    FSDataOutputStream out = fs.create(p);
    out.write("content".getBytes("UTF-8"));
    /*[*/out.hflush();/*]*/
    assertThat(fs.getFileStatus(p).getLen(), is(((long) "content".length())));
    // ^^ CoherencyModelTest-VisibleAfterHFlush
    out.close();
    assertThat(fs.delete(p, true), is(true));
  }

  /**
   * 验证 {@code hsync()} 之后内容可见
   *
   * <p>{@code hsync()} 比 {@code hflush()} 更严格：
   * 它额外要求数据已落盘到 DataNode 的物理磁盘
   *
   * @throws IOException 文件系统操作失败
   */
  @Test
  public void fileContentIsVisibleAfterHSync() throws IOException {
    Path p = new Path("p");
    FSDataOutputStream out = fs.create(p);
    out.write("content".getBytes("UTF-8"));
    /*[*/out.hsync();/*]*/
    assertThat(fs.getFileStatus(p).getLen(), is(((long) "content".length())));
    out.close();
    assertThat(fs.delete(p, true), is(true));
  }

  /**
   * 验证本地文件系统上 flush 与 sync 的区别
   *
   * <p>这是 HDFS 一致性模型的对照实验：
   * 本地文件在 {@code flush()} 后长度立即可见（数据已交给操作系统），
   * 而 {@code sync()} 进一步保证已落盘。HDFS 的 {@code hflush}/{@code hsync}
   * 正是为了对齐这两种语义而设计的
   *
   * @throws IOException 文件操作失败
   */
  @Test
  public void localFileContentIsVisibleAfterFlushAndSync() throws IOException {
    File localFile = File.createTempFile("tmp", "");
    assertThat(localFile.exists(), is(true));
    // vv CoherencyModelTest-LocalFileVisibleAfterFlushAndSync
    FileOutputStream out = new FileOutputStream(localFile);
    out.write("content".getBytes("UTF-8"));
    out.flush(); // flush to operating system
    out.getFD().sync(); // sync to disk
    assertThat(localFile.length(), is(((long) "content".length())));
    // ^^ CoherencyModelTest-LocalFileVisibleAfterFlushAndSync
    out.close();
    assertThat(localFile.delete(), is(true));
  }
  
  /**
   * 验证关闭文件后内容可见
   *
   * <p>{@code close()} 隐含一次完整的同步，
   * 因此关闭后内容必定可见——这是最简单也最安全的写法，
   * 代价是无法在写入过程中就读取到数据
   *
   * @throws IOException 文件系统操作失败
   */
  @Test
  public void fileContentIsVisibleAfterClose() throws IOException {
    // vv CoherencyModelTest-VisibleAfterClose
    Path p = new Path("p");
    OutputStream out = fs.create(p);
    out.write("content".getBytes("UTF-8"));
    /*[*/out.close();/*]*/
    assertThat(fs.getFileStatus(p).getLen(), is(((long) "content".length())));
    // ^^ CoherencyModelTest-VisibleAfterClose
    assertThat(fs.delete(p, true), is(true));
  }

}
