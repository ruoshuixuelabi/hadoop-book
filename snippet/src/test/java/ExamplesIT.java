// cc ExamplesIT 运行示例程序并验证输出是否正确的集成测试

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import junitx.framework.FileAssert;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.OrFileFilter;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * ExamplesIT - 示例程序的集成测试
 *
 * <p>此测试运行 Hadoop Book 中的示例程序,并验证其输出是否符合预期。
 * 它读取每个 input.txt 文件作为脚本执行,然后检查输出是否与 output 目录中的文件一致。</p>
 *
 * <p><b>测试流程:</b></p>
 * <ol>
 *   <li>从 example.chapters 属性读取要测试的章节目录</li>
 *   <li>遍历每个章节的 src/main/examples 目录</li>
 *   <li>对每个示例目录执行 input.txt 脚本</li>
 *   <li>比较实际输出与预期输出</li>
 * </ol>
 *
 * <p><b>配置属性:</b></p>
 * <ul>
 *   <li>hadoop.book.basedir: 项目根目录 (默认 /Users/tom/book-workspace/hadoop-book)</li>
 *   <li>example.mode: 运行模式 (默认 local,可选 hadoop)</li>
 *   <li>example.chapters: 要测试的章节列表 (默认 ch02-mr-intro,ch05-io,ch12-avro,等)</li>
 * </ul>
 *
 * <p><b>环境要求:</b></p>
 * <ul>
 *   <li>HADOOP_HOME: 必须设置,指向 Hadoop 安装目录</li>
 *   <li>HADOOP_CONF_DIR: 配置文件目录 (默认 snippet/conf/{mode})</li>
 * </ul>
 *
 * @see org.junit.runners.Parameterized
 */
@RunWith(Parameterized.class)
public class ExamplesIT {

  /** 项目根目录 */
  private static final File PROJECT_BASE_DIR =
    new File(System.getProperty("hadoop.book.basedir",
        "/Users/tom/book-workspace/hadoop-book"));

  /** 运行模式属性名称 */
  private static final String MODE_PROPERTY = "example.mode";
  /** 默认运行模式 (local 或 hadoop) */
  private static final String MODE_DEFAULT = "local";

  /** 要测试的章节属性名称 */
  private static final String EXAMPLE_CHAPTERS_PROPERTY = "example.chapters";
  /** 默认要测试的章节列表 */
  private static final String EXAMPLE_CHAPTERS_DEFAULT = "ch02-mr-intro,ch05-io,ch12-avro,ch06-mr-dev,ch08-mr-types,ch09-mr-features";

  /** 隐藏文件过滤器 - 排除以点开头的文件和目录 */
  private static final IOFileFilter HIDDEN_FILE_FILTER =
    new OrFileFilter(HiddenFileFilter.HIDDEN, new PrefixFileFilter("_"));
  /** 非隐藏文件过滤器 */
  private static final IOFileFilter NOT_HIDDEN_FILE_FILTER =
    new NotFileFilter(HIDDEN_FILE_FILTER);

  /**
   * data - 测试参数化数据源
   *
   * <p>此方法返回所有要测试的示例目录列表。
   * 每个目录对应一个测试用例,测试运行时作为参数传入。</p>
   *
   * <p><b>扫描逻辑:</b></p>
   * <ol>
   *   <li>从属性读取章节列表,用逗号分隔</li>
   *   <li>对每个章节,扫描 src/main/examples 目录</li>
   *   <li>收集所有子目录作为测试用例</li>
   * </ol>
   *
   * @return Object[][] - 每个元素是一个包含 File (示例目录) 的数组
   */
  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> data = new ArrayList<Object[]>();
    // 从系统属性获取要测试的章节目录列表
    String exampleDirs = System.getProperty(EXAMPLE_CHAPTERS_PROPERTY,
        EXAMPLE_CHAPTERS_DEFAULT);
    int i = 0;
    // 遍历每个章节目录
    for (String dirName : Splitter.on(',').split(exampleDirs)) {
      // 构建章节的 examples 目录路径
      File dir = new File(new File(PROJECT_BASE_DIR, dirName),
          "src/main/examples");
      if (!dir.exists()) {
        // 如果目录不存在,测试失败
        fail(dir + " does not exist");
      }
      // 遍历目录下所有文件和子目录
      for (File file : dir.listFiles()) {
        if (file.isDirectories()) {
          // 只处理目录,每个子目录是一个示例
          data.add(new Object[] { file });
          // 打印索引和目录名,便于查看测试进度
          System.out.printf("%s: %s\n", i++, file);
        }
      }
    }
    return data;
  }

  /** 当前测试的示例目录 (参数化) */
  private File example;
  /** 实际输出目录 */
  private File actualOutputDir = new File(PROJECT_BASE_DIR, "output");
  /** 环境变量缓存 */
  private static Map<String, String> env;
  /** Hadoop 版本号 */
  private static String version;
  /** Hadoop 主版本号 (1, 2, 或 0.x) */
  private static String majorVersion;
  /** 运行模式 (local 或 hadoop) */
  private static String mode;

  /**
   * 构造函数 - 接收参数化的示例目录
   *
   * @param example 示例目录
   */
  public ExamplesIT(File example) {
    this.example = example;
  }

  /**
   * setUpClass - 类级别初始化
   *
   * <p>在所有测试运行前执行一次,用于:</p>
   * <ol>
   *   <li>读取运行模式</li>
   *   <li>验证 HADOOP_HOME 环境变量</li>
   *   <li>设置测试环境变量</li>
   *   <li>获取并解析 Hadoop 版本号</li>
   * </ol>
   *
   * @throws IOException 如果发生 I/O 错误
   */
  @SuppressWarnings("unchecked")
  @BeforeClass
  public static void setUpClass() throws IOException {
    // 获取运行模式
    mode = System.getProperty(MODE_PROPERTY, MODE_DEFAULT);
    System.out.printf("mode=%s\n", mode);

    // 验证 HADOOP_HOME 环境变量必须设置
    String hadoopHome = System.getenv("HADOOP_HOME");
    assertNotNull("Export the HADOOP_HOME environment variable " +
        "to run the snippet tests", hadoopHome);

    // 获取当前进程的环境变量
    env = new HashMap<String, String>(EnvironmentUtils.getProcEnvironment());

    // 设置测试所需的环境变量
    env.put("HADOOP_HOME", hadoopHome);
    // 将 hadoop bin 目录添加到 PATH
    env.put("PATH", env.get("HADOOP_HOME") + "/bin" + ":" + env.get("PATH"));
    // 设置 Hadoop 配置目录
    env.put("HADOOP_CONF_DIR", "snippet/conf/" + mode);
    // 设置 classpath 优先使用本地类
    env.put("HADOOP_USER_CLASSPATH_FIRST", "true");
    // 设置 Hadoop classpath
    env.put("HADOOP_CLASSPATH", "hadoop-examples.jar:avro-examples.jar");

    System.out.printf("HADOOP_HOME=%s\n", hadoopHome);

    // 执行 hadoop version 命令并解析输出
    String versionOut = execute(hadoopHome + "/bin/hadoop version");
    for (String line : Splitter.on('\n').split(versionOut)) {
      // 使用正则表达式匹配 "Hadoop x.y.z" 格式
      Matcher matcher = Pattern.compile("^Hadoop (.+)+$").matcher(line);
      if (matcher.matches()) {
        version = matcher.group(1);
      }
    }
    assertNotNull("Version not found", version);
    System.out.printf("version=%s\n", version);

    // 提取主版本号 (第一个数字)
    majorVersion = version.substring(0, version.indexOf('.'));

    // 特殊处理 0.20.x, 0.21.x, 0.22.x, 0.23.x 版本
    // 将它们映射到主版本 1 或 2
    if ("0".equals(majorVersion)) {
      if (version.startsWith("0.20")) {
        majorVersion = "1";
      } else if (version.startsWith("0.21") || version.startsWith("0.22") ||
          version.startsWith("0.23")) {
        majorVersion = "2";
      }
    }
    assertNotNull("Major version not found", majorVersion);
    System.out.printf("majorVersion=%s\n", majorVersion);

  }

  /**
   * setUp - 每个测试方法运行前的准备
   *
   * <p>执行测试环境设置脚本 setup.sh</p>
   *
   * @throws IOException 如果发生 I/O 错误
   */
  @Before
  public void setUp() throws IOException {
    // 跳过以 .ignore 结尾的示例
    assumeTrue(!example.getPath().endsWith(".ignore"));
    // 执行设置脚本
    execute(new File("src/test/resources/setup.sh").getAbsolutePath());
  }

  /**
   * test - 主测试方法
   *
   * <p><b>测试步骤:</b></p>
   * <ol>
   *   <li>查找示例的基础目录 (考虑版本和模式)</li>
   *   <li>读取 input.txt 作为命令执行</li>
   *   <li>收集实际输出</li>
   *   <li>与预期输出比较</li>
   * </ol>
   *
   * <p><b>文件比较逻辑:</b></p>
   * <ul>
   *   <li>.gz 文件: 先解压再比较</li>
   *   <li>.avro 文件: 只比较文件长度 (因为有随机同步标记)</li>
   *   <li>其他文件: 直接比较内容</li>
   * </ul>
   *
   * @throws Exception 如果测试失败
   */
  @Test
  public void test() throws Exception {
    System.out.println("Running " + example);

    // 查找示例的基础目录
    File exampleDir = findBaseExampleDirectory(example);
    // 查找 input.txt 文件
    File inputFile = new File(exampleDir, "input.txt");
    System.out.println("Running input " + inputFile);

    // 执行 input.txt 中的命令
    String systemOut = execute(inputFile.getAbsolutePath());
    System.out.println(systemOut);

    // 复制输出文件到预期输出目录
    execute(new File("src/test/resources/copyoutput.sh").getAbsolutePath());

    // 预期输出目录
    File expectedOutputDir = new File(exampleDir, "output");
    // 如果预期输出目录不存在,说明是新创建的,测试失败
    if (!expectedOutputDir.exists()) {
      // 将实际输出复制到预期输出目录作为备份
      FileUtils.copyDirectory(actualOutputDir, expectedOutputDir);
      fail(expectedOutputDir  + " does not exist - creating.");
    }

    // 获取所有预期输出文件 (排除隐藏文件)
    List<File> expectedParts = Lists.newArrayList(
        FileUtils.listFiles(expectedOutputDir, NOT_HIDDEN_FILE_FILTER,
            NOT_HIDDEN_FILE_FILTER));
    // 获取所有实际输出文件
    List<File> actualParts = Lists.newArrayList(
        FileUtils.listFiles(actualOutputDir, NOT_HIDDEN_FILE_FILTER,
            NOT_HIDDEN_FILE_FILTER));

    // 验证文件数量一致
    assertEquals("Number of parts (got " + actualParts + ")",
        expectedParts.size(), actualParts.size());

    // 逐个比较输出文件
    for (int i = 0; i < expectedParts.size(); i++) {
      File expectedFile = expectedParts.get(i);
      File actualFile = actualParts.get(i);

      // 根据文件类型选择比较方式
      if (expectedFile.getPath().endsWith(".gz")) {
        // GZIP 文件: 先解压再比较
        File expectedDecompressed = decompress(expectedFile);
        File actualDecompressed = decompress(actualFile);
        FileAssert.assertEquals(expectedFile.toString(),
            expectedDecompressed, actualDecompressed);
      } else if (expectedFile.getPath().endsWith(".avro")) {
        // Avro 文件: 因为有随机同步标记,只比较长度
        assertEquals("Avro file length", expectedFile.length(),
            actualFile.length());
      } else {
        // 其他文件: 直接比较内容
        FileAssert.assertEquals(expectedFile.toString(),
            expectedFile, actualFile);
      }
    }
    System.out.println("Completed " + example);
  }

  /**
   * findBaseExampleDirectory - 查找示例的基础目录
   *
   * <p>按优先级查找输入文件:</p>
   * <ol>
   *   <li>base/{version}/{mode}/input.txt</li>
   *   <li>base/{version}/input.txt</li>
   *   <li>base/{majorVersion}/{mode}/input.txt</li>
   *   <li>base/{majorVersion}/input.txt</li>
   *   <li>base/{mode}/input.txt</li>
   *   <li>base/input.txt (兜底)</li>
   * </ol>
   *
   * @param example 示例目录
   * @return 包含 input.txt 的基础目录
   */
  private File findBaseExampleDirectory(File example) {
    // 候选目录列表,按优先级排序
    File[] candidates = {
        // 版本和模式都特定
        new File(new File(example, version), mode),
        // 只有版本特定
        new File(example, version),
        // 主版本和模式特定
        new File(new File(example, majorVersion), mode),
        // 只有主版本特定
        new File(example, majorVersion),
        // 只有模式特定
        new File(example, mode),
    };

    // 遍历查找第一个存在的目录
    for (File candidate : candidates) {
      if (candidate.exists()) {
        File inputFile = new File(candidate, "input.txt");
        // 如果没有 input.txt 文件,则跳过此候选
        assumeTrue(inputFile.exists());
        return candidate;
      }
    }
    // 兜底返回原始目录
    return example;
  }

  /**
   * execute - 执行 bash 命令
   *
   * <p>使用 Apache Commons Exec 执行命令行,并捕获标准输出。</p>
   *
   * <p><b>执行环境:</b></p>
   * <ul>
   *   <li>工作目录: 项目根目录</li>
   *   <li>环境变量: setUpClass 中设置的变量</li>
   * </ul>
   *
   * @param commandLine 要执行的命令 (会被包装为 /bin/bash -c "commandLine")
   * @return 命令的标准输出
   * @throws ExecuteException 如果命令执行失败
   * @throws IOException 如果发生 I/O 错误
   */
  private static String execute(String commandLine) throws ExecuteException, IOException {
    // 用于捕获标准输出的字节数组流
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    // 创建流处理器,将进程输出重定向到 stdout
    PumpStreamHandler psh = new PumpStreamHandler(stdout);

    // 解析命令行
    CommandLine cl = CommandLine.parse("/bin/bash " + commandLine);
    // 创建执行器
    DefaultExecutor exec = new DefaultExecutor();
    exec.setWorkingDirectory(PROJECT_BASE_DIR);
    exec.setStreamHandler(psh);

    try {
      // 执行命令
      exec.execute(cl, env);
    } catch (ExecuteException e) {
      // 命令失败时,打印输出并抛出异常
      System.out.println(stdout.toString());
      throw e;
    } catch (IOException e) {
      // IO 错误时,打印输出并抛出异常
      System.out.println(stdout.toString());
      throw e;
    }
    return stdout.toString();
  }

  /**
   * decompress - 解压 GZIP 文件
   *
   * <p>创建临时文件存储解压后的内容,文件会在 JVM 退出时自动删除。</p>
   *
   * @param file GZIP 压缩文件
   * @return 解压后的临时文件
   * @throws IOException 如果解压失败
   */
  private File decompress(File file) throws IOException {
    // 创建临时文件,使用类名作为前缀
    File decompressed = File.createTempFile(getClass().getSimpleName(), ".txt");
    // 设置 JVM 退出时删除临时文件
    decompressed.deleteOnExit();

    // 创建 GZIP 输入流
    final GZIPInputStream in = new GZIPInputStream(new FileInputStream(file));
    try {
      // 使用 Guava 的 Files.copy 复制数据到临时文件
      Files.copy(new InputSupplier<InputStream>() {
          public InputStream getInput() throws IOException {
            return in;
          }
      }, decompressed);
    } finally {
      // 确保输入流被关闭
      in.close();
    }
    return decompressed;
  }

}
