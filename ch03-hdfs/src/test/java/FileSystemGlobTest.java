import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.*;

/**
 * HDFS路径匹配（Glob）功能测试类。
 *
 * <p>此类测试HDFS的globStatus方法，支持Unix风格的通配符模式匹配。
 * globStatus返回匹配指定模式的路径列表。</p>
 *
 * <p>支持的通配符：</p>
 * <ul>
 *   <li><b>*</b>：匹配零个或多个任意字符</li>
 *   <li><b>?</b>：匹配单个任意字符</li>
 *   <li><b>[abc]</b>：匹配字符集合中的任意一个</li>
 *   <li><b>[a-z]</b>：匹配字符范围内的任意一个</li>
 *   <li><b>[^abc]</b>：排除字符集合</li>
 *   <li><b>{a,b}</b>：匹配多个模式的并集</li>
 * </ul>
 *
 * <p>测试结构：</p>
 * <ul>
 *   <li>glob(): 测试标准通配符匹配</li>
 *   <li>regexIncludes(): 测试正则表达式包含过滤</li>
 *   <li>regexExcludes(): 测试正则表达式排除过滤</li>
 *   <li>regexExcludesWithRegexExcludePathFilter(): 测试排除过滤器</li>
 *   <li>testDateRange(): 测试日期范围过滤器</li>
 * </ul>
 *
 * @see PathFilter 路径过滤器接口
 * @see RegexPathFilter 正则表达式路径过滤器
 * @see DateRangePathFilter 日期范围路径过滤器
 */
public class FileSystemGlobTest {

  // 测试基础路径：/tmp/FileSystemGlobTest
  private static final String BASE_PATH = "/tmp/" +
    FileSystemGlobTest.class.getSimpleName();

  // 测试使用的FileSystem实例
  private FileSystem fs;

  /**
   * 测试前准备：创建测试目录结构。
   *
   * <p>创建的目录结构用于测试glob匹配：</p>
   * <pre>
   *   /tmp/FileSystemGlobTest/
   *   ├── 2007/12/30
   *   ├── 2007/12/31
   *   ├── 2008/01/01
   *   └── 2008/01/02
   * </pre>
   *
   * @throws Exception 如果准备失败
   */
  @Before
  public void setUp() throws Exception {
    fs = FileSystem.get(new Configuration());
    // 创建测试目录结构
    fs.mkdirs(new Path(BASE_PATH, "2007/12/30"));
    fs.mkdirs(new Path(BASE_PATH, "2007/12/31"));
    fs.mkdirs(new Path(BASE_PATH, "2008/01/01"));
    fs.mkdirs(new Path(BASE_PATH, "2008/01/02"));
  }

  /**
   * 测试后清理：删除测试目录。
   *
   * @throws Exception 如果清理失败
   */
  @After
  public void tearDown() throws Exception {
    // 递归删除整个测试目录
    fs.delete(new Path(BASE_PATH), true);
  }

  /**
   * 测试：标准通配符模式匹配。
   *
   * <p>测试各种glob模式：</p>
   *
   * @throws Exception 如果测试失败
   */
  @Test
  public void glob() throws Exception {
    // 匹配任意年份
    assertThat(glob("/*"), is(paths("/2007", "/2008")));
    // 匹配任意年份/月份
    assertThat(glob("/*/*"), is(paths("/2007/12", "/2008/01")));
    // 匹配任意月份下的30或31日
    assertThat(glob("/*/12/*"), is(paths("/2007/12/30", "/2007/12/31")));
    // ? 匹配单个字符：/2007, /2008
    assertThat(glob("/200?"), is(paths("/2007", "/2008")));
    // [78] 匹配字符集合：/2007, /2008
    assertThat(glob("/200[78]"), is(paths("/2007", "/2008")));
    // [7-8] 匹配字符范围：/2007, /2008
    assertThat(glob("/200[7-8]"), is(paths("/2007", "/2008")));
    // [^...] 排除字符：排除0,1,2,3,4,5,6,9，保留7,8
    assertThat(glob("/200[^01234569]"), is(paths("/2007", "/2008")));

    // {31,01} 匹配多个模式：2007/12/31 和 2008/01/01
    assertThat(glob("/*/*/{31,01}"), is(paths("/2007/12/31", "/2008/01/01")));
    // {30,31} 匹配：2007/12/30 和 2007/12/31
    assertThat(glob("/*/*/3{0,1}"), is(paths("/2007/12/30", "/2007/12/31")));

    // 组合模式：2007/12/31 和 2008/01/01
    assertThat(glob("/*/{12/31,01/01}"), is(paths("/2007/12/31", "/2008/01/01")));
  }

  /**
   * 测试：使用正则表达式过滤器包含匹配。
   *
   * <p>RegexPathFilter(regex, true) 包含模式：
   * 只返回匹配正则表达式的路径。</p>
   *
   * @throws Exception 如果测试失败
   */
  @Test
  public void regexIncludes() throws Exception {
    // 包含匹配2007结尾的路径
    assertThat(glob("/*", new RegexPathFilter("^.*/2007$")), is(paths("/2007")));
    // 精确匹配：2007/12/31
    assertThat(glob("/*/*/*", new RegexPathFilter("^.*/2007/12/31$")), is(paths("/2007/12/31")));
    // 可选匹配：2007 或 2007/12 或 2007/12/31
    assertThat(glob("/*/*/*", new RegexPathFilter("^.*/2007(/12(/31)?)?$")), is(paths("/2007/12/31")));
  }

  /**
   * 测试：使用正则表达式过滤器排除匹配。
   *
   * <p>RegexPathFilter(regex, false) 排除模式：
   * 返回不匹配正则表达式的路径（排除匹配到的）。</p>
   *
   * @throws Exception 如果测试失败
   */
  @Test
  public void regexExcludes() throws Exception {
    // 排除2007，返回2008
    assertThat(glob("/*", new RegexPathFilter("^.*/2007$", false)), is(paths("/2008")));
    // 排除2007/12/31，返回2007/12/30
    assertThat(glob("/2007/*/*", new RegexPathFilter("^.*/2007/12/31$", false)), is(paths("/2007/12/30")));
  }

  /**
   * 测试：使用RegexExcludePathFilter排除匹配。
   *
   * <p>RegexExcludePathFilter是正则排除的便捷类，
   * 等价于RegexPathFilter(regex, false)。</p>
   *
   * @throws Exception 如果测试失败
   */
  @Test
  public void regexExcludesWithRegexExcludePathFilter() throws Exception {
    // 排除2007，返回2008
    assertThat(glob("/*", new RegexExcludePathFilter("^.*/2007$")), is(paths("/2008")));
    // 排除2007/12/31，返回2007/12/30
    assertThat(glob("/2007/*/*", new RegexExcludePathFilter("^.*/2007/12/31$")), is(paths("/2007/12/30")));
  }

  /**
   * 测试：使用日期范围过滤器。
   *
   * <p>DateRangePathFilter根据路径中的日期部分过滤，
   * 只返回日期在指定范围内的路径。</p>
   *
   * <p>测试用例：</p>
   * <ul>
   *   <li>起始日期：2007/12/31</li>
   *   <li>结束日期：2008/01/01</li>
   *   <li>包含：2007/12/31 和 2008/01/01</li>
   * </ul>
   *
   * @throws Exception 如果测试失败
   */
  @Test
  public void testDateRange() throws Exception {
    // 创建日期范围过滤器：2007/12/31 到 2008/01/01
    DateRangePathFilter filter = new DateRangePathFilter(date("2007/12/31"),
        date("2008/01/01"));
    // 验证只返回日期在范围内的路径
    assertThat(glob("/*/*/*", filter), is(paths("/2007/12/31", "/2008/01/01")));
  }

  /**
   * 辅助方法：使用glob模式匹配路径。
   *
   * @param pattern glob模式字符串
   * @return 匹配的模式对应路径集合
   * @throws IOException 如果匹配失败
   */
  private Set<Path> glob(String pattern) throws IOException {
    // globStatus返回FileStatus数组，stat2Paths转换为Path数组
    return new HashSet<Path>(Arrays.asList(
        FileUtil.stat2Paths(fs.globStatus(new Path(BASE_PATH + pattern)))));
  }

  /**
   * 辅助方法：使用glob模式匹配路径并应用PathFilter。
   *
   * @param pattern glob模式字符串
   * @param pathFilter 路径过滤器
   * @return 匹配的模式且通过过滤器过滤的路径集合
   * @throws IOException 如果匹配失败
   */
  private Set<Path> glob(String pattern, PathFilter pathFilter) throws IOException {
    return new HashSet<Path>(Arrays.asList(
        FileUtil.stat2Paths(fs.globStatus(new Path(BASE_PATH + pattern), pathFilter))));
  }

  /**
   * 辅助方法：将路径字符串转换为包含完整URI的Path集合。
   *
   * @param pathStrings 相对路径字符串数组
   * @return Path集合，每个Path包含完整的file: URI
   */
  private Set<Path> paths(String... pathStrings) {
    Path[] paths = new Path[pathStrings.length];
    for (int i = 0; i < paths.length; i++) {
      // 构建完整的file: URI
      paths[i] = new Path("file:" + BASE_PATH + pathStrings[i]);
    }
    return new HashSet<Path>(Arrays.asList(paths));
  }

  /**
   * 辅助方法：解析日期字符串为Date对象。
   *
   * @param date 日期字符串，格式为yyyy/MM/dd
   * @return Date对象
   * @throws ParseException 如果日期格式不正确
   */
  private Date date(String date) throws ParseException {
    return new SimpleDateFormat("yyyy/MM/dd").parse(date);
  }
}
