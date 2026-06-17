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
 * 文件状态信息测试类。
 *
 * <p>此类测试FileStatus类的各种属性，包括：</p>
 * <ul>
 *   <li>文件属性（长度、块大小、副本数）</li>
 *   <li>时间属性（修改时间）</li>
 *   <li>所有权（所有者、组）</li>
 *   <li>权限信息</li>
 *   <li>路径类型（文件或目录）</li>
 * </ul>
 *
 * <p>FileStatus包含的完整信息：</p>
 * <table>
 *   <tr><th>方法</th><th>说明</th></tr>
 *   <tr><td>getPath()</td><td>文件路径</td></tr>
 *   <tr><td>isDirectory()</td><td>是否为目录</td></tr>
 *   <tr><td>getLen()</td><td>文件长度（字节）</td></tr>
 *   <tr><td>getModificationTime()</td><td>修改时间（毫秒时间戳）</td></tr>
 *   <tr><td>getReplication()</td><td>副本数（目录为0）</td></tr>
 *   <tr><td>getBlockSize()</td><td>块大小（目录为0）</td></tr>
 *   <tr><td>getOwner()</td><td>所有者用户名</td></tr>
 *   <tr><td>getGroup()</td><td>所属组</td></tr>
 *   <tr><td>getPermission()</td><td>权限（FsPermission对象）</td></tr>
 * </table>
 *
 * <p>测试环境使用MiniDFSCluster（嵌入式HDFS集群）。</p>
 *
 * @see FileStatus 文件状态信息类
 * @see MiniDFSCluster 嵌入式HDFS测试集群
 */
public class ShowFileStatusTest {

    // MiniDFSCluster：嵌入式HDFS集群，用于测试
    private MiniDFSCluster cluster;
    private FileSystem fs;

    /**
     * 测试前准备：创建嵌入式HDFS集群和测试文件。
     *
     * <p>初始化步骤：</p>
     * <ol>
     *   <li>创建Configuration对象</li>
     *   <li>设置test.build.data系统属性</li>
     *   <li>构建并启动MiniDFSCluster</li>
     *   <li>获取FileSystem实例</li>
     *   <li>创建测试目录/dir和文件/dir/file（内容为"content"）</li>
     * </ol>
     *
     * @throws IOException 如果初始化失败
     */
    @Before
    public void setUp() throws IOException {
        Configuration conf = new Configuration();
        if (System.getProperty("test.build.data") == null) {
            System.setProperty("test.build.data", "/tmp");
        }
        // 构建并启动嵌入式HDFS集群
        cluster = new MiniDFSCluster.Builder(conf).build();
        fs = cluster.getFileSystem();

        // 创建测试目录和文件
        // 文件内容为"content"（7个字节）
        OutputStream out = fs.create(new Path("/dir/file"));
        out.write("content".getBytes("UTF-8"));
        out.close();
    }

    /**
     * 测试后清理：关闭文件系统和集群。
     *
     * @throws IOException 如果清理失败
     */
    @After
    public void tearDown() throws IOException {
        if (fs != null) {
            fs.close();
        }
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    /**
     * 测试：访问不存在的文件应抛出FileNotFoundException。
     *
     * <p>通过@Expected注解验证异常抛出。</p>
     *
     * @throws IOException 期望抛出FileNotFoundException
     */
    @Test(expected = FileNotFoundException.class)
    public void throwsFileNotFoundForNonExistentFile() throws IOException {
        // 尝试获取不存在文件的状态，应抛出异常
        fs.getFileStatus(new Path("no-such-file"));
    }

    /**
     * 测试：获取文件状态信息。
     *
     * <p>验证FileStatus返回的各类文件属性是否正确：</p>
     * <ul>
     *   <li>路径：/dir/file</li>
     *   <li>是否为目录：false</li>
     *   <li>文件长度：7字节（"content"）</li>
     *   <li>修改时间：不超过当前时间</li>
     *   <li>副本数：1（MiniDFSCluster默认）</li>
     *   <li>块大小：128MB（HDFS默认）</li>
     *   <li>所有者：当前用户名</li>
     *   <li>所属组：supergroup</li>
     *   <li>权限：rw-r--r--</li>
     * </ul>
     *
     * @throws IOException 如果测试失败
     */
    @Test
    public void fileStatusForFile() throws IOException {
        Path file = new Path("/dir/file");
        // 获取文件状态
        FileStatus stat = fs.getFileStatus(file);

        // 验证路径
        assertThat(stat.getPath().toUri().getPath(), is("/dir/file"));
        // 验证是否为目录
        assertThat(stat.isDirectory(), is(false));
        // 验证文件长度（"content"共7个字节）
        assertThat(stat.getLen(), is(7L));
        // 验证修改时间（应小于等于当前时间）
        assertThat(stat.getModificationTime(), is(lessThanOrEqualTo(System.currentTimeMillis())));
        // 验证副本数（嵌入式集群默认为1）
        assertThat(stat.getReplication(), is((short) 1));
        // 验证块大小（HDFS默认128MB）
        assertThat(stat.getBlockSize(), is(128 * 1024 * 1024L));
        // 验证所有者（当前运行用户）
        assertThat(stat.getOwner(), is(System.getProperty("user.name")));
        // 验证所属组（嵌入式集群默认为supergroup）
        assertThat(stat.getGroup(), is("supergroup"));
        // 验证权限（rw-r--r--）
        assertThat(stat.getPermission().toString(), is("rw-r--r--"));
    }

    /**
     * 测试：获取目录状态信息。
     *
     * <p>验证目录的FileStatus属性：</p>
     * <ul>
     *   <li>路径：/dir</li>
     *   <li>是否为目录：true</li>
     *   <li>目录长度：0（目录不计算内容长度）</li>
     *   <li>副本数：0（目录不是文件）</li>
     *   <li>块大小：0（目录不是文件）</li>
     *   <li>权限：rwxr-xr-x</li>
     * </ul>
     *
     * @throws IOException 如果测试失败
     */
    @Test
    public void fileStatusForDirectory() throws IOException {
        Path dir = new Path("/dir");
        FileStatus stat = fs.getFileStatus(dir);

        // 验证路径
        assertThat(stat.getPath().toUri().getPath(), is("/dir"));
        // 验证是否为目录
        assertThat(stat.isDirectory(), is(true));
        // 验证目录长度（目录长度为0）
        assertThat(stat.getLen(), is(0L));
        // 验证修改时间
        assertThat(stat.getModificationTime(), is(lessThanOrEqualTo(System.currentTimeMillis())));
        // 验证副本数（目录副本数为0）
        assertThat(stat.getReplication(), is((short) 0));
        // 验证块大小（目录块大小为0）
        assertThat(stat.getBlockSize(), is(0L));
        // 验证所有者
        assertThat(stat.getOwner(), is(System.getProperty("user.name")));
        // 验证所属组
        assertThat(stat.getGroup(), is("supergroup"));
        // 验证权限（rwxr-xr-x：目录有执行权限）
        assertThat(stat.getPermission().toString(), is("rwxr-xr-x"));
    }

}
// ^^ ShowFileStatusTest
