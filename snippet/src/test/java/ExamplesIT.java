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
 * 跑一遍书里的示例，验证它们的输出跟预期一致
 *
 * <p>每个示例目录下都放着一份 {@code input.txt}，
 * 把它当成脚本跑一遍，再把跑出来的结果跟 {@code output} 目录里的文件逐个比对
 * <p>这是一个「集成测试」（文件名以 {@code IT} 结尾），
 * 平时不跟 {@code mvn test} 一起跑，
 * 因为它需要真的把 Hadoop 命令拉起来
 * <p>这是 JUnit 的参数化测试：{@code data()} 把每个示例目录收集成一个参数，
 * 框架会为每个参数各造一个实例，各跑一遍 {@link #test()}
 *
 * This test runs the examples and checks that they produce the expected output.
 * It takes each input.txt file and runs it as a script, then tests that the
 * output produced is the same as all the files in output.
 */
@RunWith(Parameterized.class)
public class ExamplesIT {

  // 项目的根目录：既是算子运行的起跑线，也是找示例目录的基准
  private static final File PROJECT_BASE_DIR =
    new File(System.getProperty("hadoop.book.basedir",
        "/Users/tom/book-workspace/hadoop-book"));
  
  // 运行模式这个配置项的名字
  private static final String MODE_PROPERTY = "example.mode";
  // 默认跑本地模式
  private static final String MODE_DEFAULT = "local";
  
  // 指定要跑哪几章示例的配置项名字
  private static final String EXAMPLE_CHAPTERS_PROPERTY = "example.chapters";
  // 默认只跑这几章
  private static final String EXAMPLE_CHAPTERS_DEFAULT = "ch02-mr-intro,ch05-io,ch12-avro,ch06-mr-dev,ch08-mr-types,ch09-mr-features";

  // 隐藏文件，以及以 _ 开头的临时文件，都算「不算数」
  private static final IOFileFilter HIDDEN_FILE_FILTER =
    new OrFileFilter(HiddenFileFilter.HIDDEN, new PrefixFileFilter("_"));
  // 它的反面：只留正常的文件
  private static final IOFileFilter NOT_HIDDEN_FILE_FILTER =
    new NotFileFilter(HIDDEN_FILE_FILTER);
  
  /**
   * 收集所有要跑的示例目录
   *
   * <p>JUnit 的参数化测试靠这个方法提供参数：
   * 每返回一个参数，框架就会用 {@link ExamplesIT} 的构造器造一个新实例，
   * 把这个参数传进去，然后把所有 {@code @Test} 方法各跑一遍
   *
   * @return 每个元素装一个示例目录的参数集合
   */
  @Parameters
  public static Collection<Object[]> data() {
    // 收集参数：一个元素就是一个示例目录
    Collection<Object[]> data = new ArrayList<Object[]>();
    // 读出要跑哪几章；没配置就用默认的那几章
    String exampleDirs = System.getProperty(EXAMPLE_CHAPTERS_PROPERTY,
        EXAMPLE_CHAPTERS_DEFAULT);
    // 计数器，用来在日志里标出这是第几个示例
    int i = 0;
    for (String dirName : Splitter.on(',').split(exampleDirs)) {
      // 拼出「根目录/章节名/src/main/examples」
      File dir = new File(new File(PROJECT_BASE_DIR, dirName),
          "src/main/examples");
      // 章节目录必须存在，否则说明工程没 checkout 完整
      if (!dir.exists()) {
        fail(dir + " does not exist");
      }
      // 遍历这一章的每一个示例
      for (File file : dir.listFiles()) {
        // 只有目录才算「一个示例」
        if (file.isDirectory()) {
          // 把示例目录当成参数交给 JUnit
          data.add(new Object[] { file });
          // so we can see which test corresponds to which file
          // 补充：打印编号，便于在测试报告里对上号
          System.out.printf("%s: %s\n", i++, file);
        }
      }
    }
    return data;
  }
  
  // 当前这个参数化用例对应的示例目录
  private File example; // parameter
  // 示例跑完之后，输出落在哪个目录
  private File actualOutputDir = new File(PROJECT_BASE_DIR, "output");
  // 要传给子进程的环境变量
  private static Map<String, String> env;
  // 完整的 Hadoop 版本号，比如 2.7.3
  private static String version;
  // 主版本号，比如 2
  private static String majorVersion;
  // 运行模式：local 或者非 local
  private static String mode;
  
  /**
   * 参数化测试的构造器
   *
   * <p>JUnit 每拿到一个参数，就会用这个构造器造一个实例
   *
   * @param example 这个用例要跑的示例目录
   */
  public ExamplesIT(File example) {
    // 记下这个实例对应哪个示例
    this.example = example;
  }
  
  /**
   * 整个测试类跑之前先做一次准备
   *
   * <p>主要是准备子进程的环境变量，
   * 再从 {@code hadoop version} 的输出里把版本号抠出来——
   * 后面找基准输出目录要用到它
   */
  @SuppressWarnings("unchecked")
  @BeforeClass
  public static void setUpClass() throws IOException {
    // 读出运行模式
    mode = System.getProperty(MODE_PROPERTY, MODE_DEFAULT);
    System.out.printf("mode=%s\n", mode);

    // 示例脚本靠这个环境变量去找 Hadoop 的命令
    String hadoopHome = System.getenv("HADOOP_HOME");
    // 没设 HADOOP_HOME 就没法跑，直接判失败
    assertNotNull("Export the HADOOP_HOME environment variable " +
        "to run the snippet tests", hadoopHome);
    // 先把当前进程的环境变量整体复制一份
    env = new HashMap<String, String>(EnvironmentUtils.getProcEnvironment());
    // 把 HADOOP_HOME 传下去
    env.put("HADOOP_HOME", hadoopHome);
    // 把 Hadoop 的 bin 目录插到 PATH 最前面
    env.put("PATH", env.get("HADOOP_HOME") + "/bin" + ":" + env.get("PATH"));
    // 指定用哪套配置：local 模式就配一套本地单机环境
    env.put("HADOOP_CONF_DIR", "snippet/conf/" + mode);
    // 让用户自己的 jar 包排在 Hadoop 的 jar 包前面
    env.put("HADOOP_USER_CLASSPATH_FIRST", "true");
    // 示例要用到的 jar 包
    env.put("HADOOP_CLASSPATH", "hadoop-examples.jar:avro-examples.jar");
    
    System.out.printf("HADOOP_HOME=%s\n", hadoopHome);
    
    // 跑一下 hadoop version，把版本号读出来
    String versionOut = execute(hadoopHome + "/bin/hadoop version");
    // 一行一行地找 "Hadoop x.y.z" 那一行
    for (String line : Splitter.on("\n").split(versionOut)) {
      // 版本号那一行的格式是固定的
      Matcher matcher = Pattern.compile("^Hadoop (.+)+$").matcher(line);
      // 匹配上了就把版本号记下来
      if (matcher.matches()) {
        // 取出版本号本身
        version = matcher.group(1);
      }
    }
    // 没找到版本号说明环境不对
    assertNotNull("Version not found", version);
    System.out.printf("version=%s\n", version);

    // 主版本号就是第一个小数点之前那部分
    majorVersion = version.substring(0, version.indexOf('.'));
    // Treat 0.2x as Hadoop 1 or 2 for the purposes of these tests
    // 补充：0.20 算 Hadoop 1，0.21～0.23 算 Hadoop 2
    if ("0".equals(majorVersion)) {
      if (version.startsWith("0.20")) {
        // 0.20 是 Hadoop 1
        majorVersion = "1";
      } else if (version.startsWith("0.21") || version.startsWith("0.22") ||
          version.startsWith("0.23")) {
        // 0.21～0.23 是 Hadoop 2
        majorVersion = "2";
      }
    }
    assertNotNull("Major version not found", majorVersion);
    System.out.printf("majorVersion=%s\n", majorVersion);

  }
  
  /**
   * 每个用例跑之前先重置一下环境
   */
  @Before
  public void setUp() throws IOException {
    // 以 .ignore 结尾的示例这次跳过
    assumeTrue(!example.getPath().endsWith(".ignore"));
    // 跑清理脚本，把上一次的残留输出删掉
    execute(new File("src/test/resources/setup.sh").getAbsolutePath());
  }
  
  /**
   * 跑一个示例，再逐个文件比对它的输出
   */
  @Test
  public void test() throws Exception {
    System.out.println("Running " + example);
    
    // 找到这个示例真正生效的基准目录
    File exampleDir = findBaseExampleDirectory(example);
    // 示例的输入就是基准目录里的 input.txt
    File inputFile = new File(exampleDir, "input.txt");
    System.out.println("Running input " + inputFile);
    
    // 把 input.txt 当脚本跑起来
    String systemOut = execute(inputFile.getAbsolutePath());
    System.out.println(systemOut);
    
    // 把刚跑出来的输出搬到 output 目录
    execute(new File("src/test/resources/copyoutput.sh").getAbsolutePath());
    
    // 期望输出放在示例目录的 output 子目录里
    File expectedOutputDir = new File(exampleDir, "output");
    // 第一次跑这个示例时还没有期望输出
    if (!expectedOutputDir.exists()) {
      // 先把这次的输出当成期望输出存下来
      FileUtils.copyDirectory(actualOutputDir, expectedOutputDir);
      // 但这次仍然判失败，提醒人去确认一下存下来的对不对
      fail(expectedOutputDir  + " does not exist - creating.");
    }
    
    // 列出期望输出里的所有文件
    List<File> expectedParts = Lists.newArrayList(
        FileUtils.listFiles(expectedOutputDir, NOT_HIDDEN_FILE_FILTER,
            NOT_HIDDEN_FILE_FILTER));
    // 列出实际输出里的所有文件
    List<File> actualParts = Lists.newArrayList(
        FileUtils.listFiles(actualOutputDir, NOT_HIDDEN_FILE_FILTER,
            NOT_HIDDEN_FILE_FILTER));
    // 两边的文件个数得一样
    assertEquals("Number of parts (got " + actualParts + ")",
        expectedParts.size(), actualParts.size());
    
    // 逐个文件比对
    for (int i = 0; i < expectedParts.size(); i++) {
      // 期望的那一个
      File expectedFile = expectedParts.get(i);
      // 实际的那一个
      File actualFile = actualParts.get(i);
      // 压缩过的要先解开再比内容
      if (expectedFile.getPath().endsWith(".gz")) {
        // 解开期望文件
        File expectedDecompressed = decompress(expectedFile);
        // 解开实际文件
        File actualDecompressed = decompress(actualFile);
        // 比对解压之后的内容
        FileAssert.assertEquals(expectedFile.toString(),
            expectedDecompressed, actualDecompressed);
      } else if (expectedFile.getPath().endsWith(".avro")) {
        // Avro 文件没法直接比内容
        // Avro files have a random sync marker
        // so just check lengths for the moment
        // 补充：Avro 的同步标记是随机生成的，所以这里只比长度
        assertEquals("Avro file length", expectedFile.length(),
            actualFile.length());
      } else {
        // 普通文件直接逐字节比
        FileAssert.assertEquals(expectedFile.toString(),
            expectedFile, actualFile);
      }
    }
    // 跑完了，打个招呼
    System.out.println("Completed " + example);
  }
  
  /**
   * 找出这个示例真正生效的基准目录
   *
   * <p>不同 Hadoop 版本、不同运行模式下的期望输出可能不一样，
   * 所以目录名按「版本 → 模式」的顺序一层层往下找
   *
   * @param example 示例目录
   * @return 找到的基准目录；一层都没匹配上就返回示例目录本身
   */
  private File findBaseExampleDirectory(File example) {
    // Look in base/<version>/<mode> then base/<version> then base/<major-version>/<mode>
    // then base/<major-version> then base/<mode>
    // 补充：依次尝试 版本/模式 → 版本 → 主版本/模式 → 主版本 → 模式，按优先级从高到低
    File[] candidates = {
        // 版本 + 模式
        new File(new File(example, version), mode),
        // 只看版本
        new File(example, version),
        // 主版本 + 模式
        new File(new File(example, majorVersion), mode),
        // 只看主版本
        new File(example, majorVersion),
        // 只看模式
        new File(example, mode),
    };
    // 按优先级挨个试
    for (File candidate : candidates) {
      // 目录存在就说明这一层匹配上了
      if (candidate.exists()) {
        // 看看这一层里有没有输入文件
        File inputFile = new File(candidate, "input.txt");
        // if no input file then skip test
        // 补充：没有 input.txt 就说明这个示例不适用，跳过（assumeTrue 不会让测试失败）
        assumeTrue(inputFile.exists());
        // 就用这一层
        return candidate;
      }
    }
    // 一层都没匹配上，就退回用示例目录本身
    return example;
  }

  /**
   * 执行一条 shell 命令，并把它的输出抓回来
   *
   * <p>所有对 Hadoop 命令的调用都走这里，
   * 这样环境变量、工作目录这些设置只需要在一处维护
   *
   * @param commandLine 要执行的命令行
   * @return 命令的标准输出
   * @throws ExecuteException 命令返回了非零退出码
   * @throws IOException 执行过程本身出了 IO 问题
   */
  private static String execute(String commandLine) throws ExecuteException, IOException {
    // 先准备一个内存流来接住命令的输出
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    // 把命令的输出接到这个流上
    PumpStreamHandler psh = new PumpStreamHandler(stdout);
    // 统一用 bash 执行，脚本里的语法才能通用
    CommandLine cl = CommandLine.parse("/bin/bash " + commandLine);
    // 默认的执行器
    DefaultExecutor exec = new DefaultExecutor();
    // 在项目根目录下执行
    exec.setWorkingDirectory(PROJECT_BASE_DIR);
    // 指定输出去哪儿
    exec.setStreamHandler(psh);
    try {
      // 用准备好的那套环境变量执行
      exec.execute(cl, env);
    } catch (ExecuteException e) {
      // 命令返回了非零退出码，先把已经产生的输出打出来方便排查
      System.out.println(stdout.toString());
      // 再把异常抛出去
      throw e;
    } catch (IOException e) {
      // 执行过程本身出了 IO 问题，同样先把输出打出来
      System.out.println(stdout.toString());
      // 再抛出去
      throw e;
    }
    // 返回命令的完整输出
    return stdout.toString();
  }

  /**
   * 把 gzip 文件解压到一个临时文件里
   *
   * <p>期望输出里的 .gz 文件必须先解开才能比内容
   *
   * @param file 要解压的 gzip 文件
   * @return 解压出来的临时文件
   * @throws IOException 解压失败
   */
  private File decompress(File file) throws IOException {
    // 建一个临时文件来接解压后的内容
    File decompressed = File.createTempFile(getClass().getSimpleName(), ".txt");
    // JVM 退出时自动删掉
    decompressed.deleteOnExit();
    // 打开 gzip 输入流
    final GZIPInputStream in = new GZIPInputStream(new FileInputStream(file));
    try {
      // 用 Guava 的 Files.copy 把流里的内容写进临时文件
      Files.copy(new InputSupplier<InputStream>() {
          /**
           * 回调：每次要读数据时都从这里拿流
           *
           * @return 输入流
           * @throws IOException 读失败
           */
          public InputStream getInput() throws IOException {
            // 始终返回同一个流
            return in;
          }
      }, decompressed);
    } finally {
      // 用完就关
      in.close();
    }
    // 返回解压后的文件
    return decompressed;
  }

}
