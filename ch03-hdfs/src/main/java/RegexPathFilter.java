import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 基于正则表达式的PathFilter实现类。
 *
 * <p>此类提供通用的正则表达式路径过滤功能，可以根据正则模式
 * 包含或排除匹配的路径。</p>
 *
 * <p>典型应用场景：</p>
 * <ul>
 *   <li>按文件名模式选择特定文件（如所有以.log结尾的文件）</li>
 *   <li>按路径模式选择目录下的特定文件</li>
 *   <li>结合Glob使用进行更复杂的路径匹配</li>
 * </ul>
 *
 * <p>使用示例：</p>
 * <pre>
 *   // 包含所有2007开头的路径
 *
 * @see PathFilter Hadoop路径过滤器接口
 * @see RegexExcludePathFilter 仅用于排除的正则过滤器
 */
public class RegexPathFilter implements PathFilter {

  // 存储正则表达式字符串
  private final String regex;
  // true表示匹配则包含，false表示匹配则排除
  private final boolean include;

  /**
   * 构造一个包含匹配路径的正则过滤器（默认行为）。
   *
   * <p>创建包含过滤器，匹配正则表达式的路径将被接受。</p>
   *
   * @param regex Java正则表达式字符串
   */
  public RegexPathFilter(String regex) {
    this(regex, true);
  }

  /**
   * 构造一个指定包含/排除行为的正则过滤器。
   *
   * @param regex   Java正则表达式字符串
   * @param include true表示匹配则包含（接受），false表示匹配则排除（拒绝）
   */
  public RegexPathFilter(String regex, boolean include) {
    this.regex = regex;
    this.include = include;
  }

  /**
   * 根据正则表达式判断路径是否被接受。
   *
   * <p>实现逻辑：</p>
   * <ul>
   *   <li>如果include为true：路径匹配正则表达式则接受</li>
   *   <li>如果include为false：路径匹配正则表达式则拒绝</li>
   * </ul>
   *
   * @param path 要检查的HDFS路径
   * @return 根据过滤策略返回是否接受该路径
   */
  public boolean accept(Path path) {
    // 使用三元表达式判断：匹配时根据include决定返回true或false
    return (path.toString().matches(regex)) ? include : !include;
  }

}
