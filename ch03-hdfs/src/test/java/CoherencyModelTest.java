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
 * HDFS一致性模型测试类。
 *
 * <p>此类测试Hadoop/HDFS的数据可见性和一致性保证。
 * HDFS与传统文件系统的一个重要区别是写入数据后的可见性问题。</p>
 *
 * <p>核心概念：</p>
 * <ul>
 *   <li><b>flush()</b>: 将用户缓冲区数据刷新到操作系统缓冲区，但不保证数据写入磁盘</li>
 *   <li><b>hflush()</b>: Hadoop特有方法，刷新数据使所有客户端可见（数据写入DataNode）</li>
 *   <li><b>hsync()</b>: Hadoop特有方法，刷新数据并同步到磁盘</li>
 *   <li><b>close()</b>: 关闭文件时HDFS会自动执行hsync操作</li>
 * </ul>
 *
 * <p>测试内容：</p>
 * <ol>
 *   <li>文件创建后立即可见</li>
 *   <li>flush()后内容不可见（HDFS行为）</li>
 *   <li>hflush()后内容可见</li>
 *   <li>hsync()后内容可见并持久化</li>
 *   <li>本地文件flush+sync后可见</li>
 *   <li>close()后内容可见</li>
 * </ol>
 *
 * <p>测试环境使用MiniDFSCluster（内存中的HDFS集群），适合单元测试。</p>
 *
 * @see MiniDFSCluster 嵌入式HDFS测试集群
 * @see FSDataOutputStream HDFS数据输出流
 */
public class CoherencyModelTest {
  // MiniDFSCluster：嵌入式HDFS集群，用于测试环境
  // 与真实的分布式HDFS行为一致，但运行在单个JVM中
  private MiniDFSCluster cluster;
  private FileSystem fs;

  /**
   * 测试前准备：创建嵌入式HDFS集群。
   *
   * <p>初始化步骤：</p>
   * <ol>
   *   <li>创建Configuration对象</li>
   *   <li>设置test.build.data系统属性（MiniDFSCluster需要）</li>
   *   <li>构建并启动MiniDFSCluster</li>
   *   <li>获取FileSystem实例用于测试</li>
   * </ol>
   *
   * @throws IOException 如果集群创建失败
   */
  @Before
  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    // MiniDFSCluster需要此系统属性来存储临时数据
    if (System.getProperty("test.build.data") == null) {
      System.setProperty("test.build.data", "/tmp");
    }
    // 构建并启动嵌入式HDFS集群
    cluster = new MiniDFSCluster.Builder(conf).build();
    // 获取集群的FileSystem用于后续测试
    fs = cluster.getFileSystem();
  }

  /**
   * 测试后清理：关闭文件系统并关闭集群。
   *
   * <p>重要：必须显式关闭FileSystem和集群，否则可能导致资源泄漏。</p>
   *
   * @throws IOException 如果关闭过程中发生错误
   */
  @After
  public void tearDown() throws IOException {
    // 关闭FileSystem
    fs.close();
    // 关闭HDFS集群
    cluster.shutdown();
  }

  /**
   * 测试：文件创建后立即可见。
   *
   * <p>验证HDFS的基本可见性保证：创建文件后，即使没有写入任何内容，
   * exists()也应该返回true。</p>
   *
   * @throws IOException 如果操作失败
   */
  @Test
  public void fileExistsImmediatelyAfterCreation() throws IOException {
    // vv CoherencyModelTest-ExistsImmediatelyAfterCreation
    Path p = new Path("p");
    // 创建文件
    fs.create(p);
    // 验证文件存在（注意：此时文件为空）
    assertThat(fs.exists(p), is(true));
    // ^^ CoherencyModelTest-ExistsImmediatelyAfterCreation
    // 测试结束后清理
    assertThat(fs.delete(p, true), is(true));
  }

  /**
   * 测试：flush()后文件内容不可见。
   *
   * <p>这是HDFS与传统文件系统的重要区别：flush()只将数据从用户缓冲区
   * 移动到操作系统缓冲区，数据尚未发送到DataNode，因此其他客户端看不到。</p>
   *
   * <p>技术细节：</p>
   * <ul>
   *   <li>flush()将数据从Java缓冲区刷新到套接字发送缓冲区</li>
   *   <li>数据可能还在内存中，未发送到DataNode</li>
 *   <li>文件元数据（如长度）可能尚未更新</li>
   * </ul>
   *
   * @throws IOException 如果操作失败
   */
  @Test
  public void fileContentIsNotVisibleAfterFlush() throws IOException {
    // vv CoherencyModelTest-NotVisibleAfterFlush
    Path p = new Path("p");
    // 创建文件获取输出流
    OutputStream out = fs.create(p);
    // 写入数据
    out.write("content".getBytes("UTF-8"));
    /*[*/out.flush();/*]*/
    // 验证：flush后，文件长度仍为0（内容不可见）
    assertThat(fs.getFileStatus(p).getLen(), is(0L));
    // ^^ CoherencyModelTest-NotVisibleAfterFlush
    // 关闭文件（close会执行sync）
    out.close();
    // 清理
    assertThat(fs.delete(p, true), is(true));
  }

  /**
   * 测试：hflush()后文件内容可见。
   *
   * <p>hflush()是Hadoop特有的方法，确保数据发送到DataNode并对所有客户端可见。
   * 这对于需要实时看到写入数据的场景很重要（如日志监控）。</p>
   *
   * <p>与flush()的区别：</p>
   * <ul>
   *   <li>flush()：只刷新到本地套接字缓冲区</li>
   *   <li>hflush()：确保数据发送到所有相关DataNode</li>
   * </ul>
   *
   * @throws IOException 如果操作失败
   */
  @Test
  public void fileContentIsVisibleAfterHFlush() throws IOException {
    // vv CoherencyModelTest-VisibleAfterHFlush
    Path p = new Path("p");
    // 需要使用FSDataOutputStream才能调用hflush()
    FSDataOutputStream out = fs.create(p);
    // 写入数据
    out.write("content".getBytes("UTF-8"));
    /*[*/out.hflush();/*]*/
    // 验证：hflush后，文件长度反映了写入的内容（内容可见）
    assertThat(fs.getFileStatus(p).getLen(), is(((long) "content".length())));
    // ^^ CoherencyModelTest-VisibleAfterHFlush
    // 关闭文件
    out.close();
    // 清理
    assertThat(fs.delete(p, true), is(true));
  }

  /**
   * 测试：hsync()后文件内容可见且持久化。
   *
   * <p>hsync()确保数据不仅对其他客户端可见，而且被持久化到磁盘。
   * 这相当于POSIX的fsync()操作。</p>
   *
   * <p>使用场景：</p>
   * <ul>
   *   <li>需要确保数据持久化的关键业务</li>
   *   <li>写checkpoints或wal（预写日志）</li>
   *   <li>应用程序认为"已提交"的数据</li>
   * </ul>
   *
   * @throws IOException 如果操作失败
   */
  @Test
  public void fileContentIsVisibleAfterHSync() throws IOException {
    Path p = new Path("p");
    FSDataOutputStream out = fs.create(p);
    out.write("content".getBytes("UTF-8"));
    /*[*/out.hsync();/*]*/
    // hsync后数据已持久化到磁盘
    assertThat(fs.getFileStatus(p).getLen(), is(((long) "content".length())));
    out.close();
    assertThat(fs.delete(p, true), is(true));
  }

  /**
   * 测试：本地文件在flush和sync后可见。
   *
   * <p>作为对比，展示传统文件系统（本地文件系统）的一致性模型。
   * 本地文件系统中，flush()配合文件描述符的sync操作可以确保数据落盘。</p>
   *
   * <p>关键区别：</p>
   * <ul>
   *   <li>HDFS flush()：数据不保证可见</li>
   *   <li>本地文件系统 flush()+sync()：数据保证可见且持久化</li>
   * </ul>
   *
   * @throws IOException 如果操作失败
   */
  @Test
  public void localFileContentIsVisibleAfterFlushAndSync() throws IOException {
    // 创建临时文件
    File localFile = File.createTempFile("tmp", "");
    // 验证文件存在
    assertThat(localFile.exists(), is(true));
    // vv CoherencyModelTest-LocalFileVisibleAfterFlushAndSync
    FileOutputStream out = new FileOutputStream(localFile);
    // 写入数据
    out.write("content".getBytes("UTF-8"));
    // 刷新到操作系统缓冲区
    out.flush();
    // 将操作系统缓冲区数据同步到磁盘
    out.getFD().sync();
    // 验证：本地文件长度现在反映了写入的内容
    assertThat(localFile.length(), is(((long) "content".length())));
    // ^^ CoherencyModelTest-LocalFileVisibleAfterFlushAndSync
    // 关闭文件
    out.close();
    // 删除临时文件
    assertThat(localFile.delete(), is(true));
  }

  /**
   * 测试：关闭文件后内容可见。
   *
   * <p>close()操作会自动执行hsync，确保所有缓冲数据被写入并可见。
   * 这是最简单可靠的持久化方式。</p>
   *
   * <p>HDFS写入流程：</p>
   * <ol>
   *   <li>创建文件，获取FSDataOutputStream</li>
   *   <li>写入数据到缓冲区</li>
   *   <li>close()时自动调用hsync()</li>
   *   <li>数据持久化，文件完整</li>
   * </ol>
   *
   * @throws IOException 如果操作失败
   */
  @Test
  public void fileContentIsVisibleAfterClose() throws IOException {
    // vv CoherencyModelTest-VisibleAfterClose
    Path p = new Path("p");
    OutputStream out = fs.create(p);
    out.write("content".getBytes("UTF-8"));
    /*[*/out.close();/*]*/
    // close()后，内容可见且持久化
    assertThat(fs.getFileStatus(p).getLen(), is(((long) "content".length())));
    // ^^ CoherencyModelTest-VisibleAfterClose
    // 清理
    assertThat(fs.delete(p, true), is(true));
  }

}
