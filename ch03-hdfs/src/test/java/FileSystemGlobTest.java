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
 * 验证 HDFS 通配符（glob）语法的测试
 *
 * <p>覆盖 {@link FileSystem#globStatus} 支持的全部通配形式：
 * {@code *}、{@code ?}、字符集 {@code [a-z]}、字符集取反 {@code [^...]}、
 * 多模式 {@code {a,b}}，以及与 {@link PathFilter} 组合使用的过滤效果
 * <p>测试在 {@link #setUp} 中建好一棵固定目录树（2007/12/30 起四层），
 * 每个用例都基于这棵树断言
 */
public class FileSystemGlobTest {
  
  /** 测试用的根目录；带上类名以避免与其他测试的目录冲突 */
  private static final String BASE_PATH = "/tmp/" +
    FileSystemGlobTest.class.getSimpleName();
  
  /** 底层文件系统实例，各测试用例共享 */
  private FileSystem fs;
  
  /**
   * 在每个用例前准备目录树
   *
   * <p>刻意构造出 2007 与 2008 两个年份、每年两个月份，
   * 使各种通配组合都能产生有区分度的结果
   *
   * @throws Exception 创建目录失败
   */
  @Before
  public void setUp() throws Exception {
    fs = FileSystem.get(new Configuration());
    fs.mkdirs(new Path(BASE_PATH, "2007/12/30"));
    fs.mkdirs(new Path(BASE_PATH, "2007/12/31"));
    fs.mkdirs(new Path(BASE_PATH, "2008/01/01"));
    fs.mkdirs(new Path(BASE_PATH, "2008/01/02"));
  }
  
  /**
   * 在每个用例后删除测试目录，避免残留数据影响后续运行
   *
   * @throws Exception 删除目录失败
   */
  @After
  public void tearDown() throws Exception {
    // 第二个参数 true 表示递归删除整个子树
    fs.delete(new Path(BASE_PATH), true);
  }
  
  /**
   * 验证各种通配符语法
   *
   * <p>{@code *} 匹配任意长度（含零长度）字符，
   * {@code ?} 匹配恰好一个字符，
   * {@code [...]} 匹配字符集中的任一字符，
   * {@code ^} 开头表示对字符集取反，
   * {@code {a,b}} 匹配花括号中列出的多个模式之一
   *
   * @throws Exception 文件系统操作失败
   */
  @Test
  public void glob() throws Exception {
    assertThat(glob("/*"), is(paths("/2007", "/2008")));
    assertThat(glob("/*/*"), is(paths("/2007/12", "/2008/01")));
    assertThat(glob("/*/12/*"), is(paths("/2007/12/30", "/2007/12/31")));
    assertThat(glob("/200?"), is(paths("/2007", "/2008")));
    assertThat(glob("/200[78]"), is(paths("/2007", "/2008")));
    assertThat(glob("/200[7-8]"), is(paths("/2007", "/2008")));
    assertThat(glob("/200[^01234569]"), is(paths("/2007", "/2008")));

    assertThat(glob("/*/*/{31,01}"), is(paths("/2007/12/31", "/2008/01/01")));
    assertThat(glob("/*/*/3{0,1}"), is(paths("/2007/12/30", "/2007/12/31")));

    assertThat(glob("/*/{12/31,01/01}"), is(paths("/2007/12/31", "/2008/01/01")));
  }
  
  /**
   * 验证「正则匹配则保留」的过滤方式
   *
   * @throws Exception 文件系统操作失败
   */
  @Test
  public void regexIncludes() throws Exception {
    assertThat(glob("/*", new RegexPathFilter("^.*/2007$")), is(paths("/2007")));
    assertThat(glob("/*/*/*", new RegexPathFilter("^.*/2007/12/31$")), is(paths("/2007/12/31")));
    assertThat(glob("/*/*/*", new RegexPathFilter("^.*/2007(/12(/31)?)?$")), is(paths("/2007/12/31")));
  }
  
  /**
   * 验证同一个过滤器类在「匹配则排除」模式下的行为
   *
   * @throws Exception 文件系统操作失败
   */
  @Test
  public void regexExcludes() throws Exception {
    assertThat(glob("/*", new RegexPathFilter("^.*/2007$", false)), is(paths("/2008")));
    assertThat(glob("/2007/*/*", new RegexPathFilter("^.*/2007/12/31$", false)), is(paths("/2007/12/30")));
  }
  
  /**
   * 验证专用排除过滤器 {@link RegexExcludePathFilter} 的效果与上一用例一致
   *
   * @throws Exception 文件系统操作失败
   */
  @Test
  public void regexExcludesWithRegexExcludePathFilter() throws Exception {
    assertThat(glob("/*", new RegexExcludePathFilter("^.*/2007$")), is(paths("/2008")));
    assertThat(glob("/2007/*/*", new RegexExcludePathFilter("^.*/2007/12/31$")), is(paths("/2007/12/30")));
  }

  /**
   * 验证按日期区间过滤路径
   *
   * @throws Exception 文件系统操作失败
   */
  @Test
  public void testDateRange() throws Exception {
    DateRangePathFilter filter = new DateRangePathFilter(date("2007/12/31"),
        date("2008/01/01"));
    assertThat(glob("/*/*/*", filter), is(paths("/2007/12/31", "/2008/01/01")));  
  } 
  
  /**
   * 用通配符查找文件并返回路径集合
   *
   * @param pattern 通配模式
   * @return 匹配到的路径集合
   * @throws IOException 文件系统操作失败
   */
  private Set<Path> glob(String pattern) throws IOException {
    return new HashSet<Path>(Arrays.asList(
        FileUtil.stat2Paths(fs.globStatus(new Path(BASE_PATH + pattern)))));
  }
  
  /**
   * 用通配符查找文件并按过滤器筛选
   *
   * @param pattern    通配模式
   * @param pathFilter 作用于匹配结果的路径过滤器
   * @return 过滤后保留的路径集合
   * @throws IOException 文件系统操作失败
   */
  private Set<Path> glob(String pattern, PathFilter pathFilter) throws IOException {
    return new HashSet<Path>(Arrays.asList(
        FileUtil.stat2Paths(fs.globStatus(new Path(BASE_PATH + pattern), pathFilter))));
  }
  
  /**
   * 把相对路径字符串转成期望的 Path 集合
   *
   * <p>加 "file:" 前缀是为了让期望值与 globStatus 返回的本地文件路径格式一致
   *
   * @param pathStrings 相对于 {@link #BASE_PATH} 的路径字符串
   * @return 对应的 Path 集合
   */
  private Set<Path> paths(String... pathStrings) {
    Path[] paths = new Path[pathStrings.length];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = new Path("file:" + BASE_PATH + pathStrings[i]);
    }
    return new HashSet<Path>(Arrays.asList(paths));
  }
  
  /**
   * 把 "yyyy/MM/dd" 格式的字符串解析为 Date
   *
   * @param date 日期字符串
   * @return 解析出的日期
   * @throws ParseException 字符串格式不合法
   */
  private Date date(String date) throws ParseException {
    return new SimpleDateFormat("yyyy/MM/dd").parse(date);
  }
}
