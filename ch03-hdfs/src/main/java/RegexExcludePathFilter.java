// cc RegexExcludePathFilter A PathFilter for excluding paths that match a regular expression
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

// vv RegexExcludePathFilter
/**
 * 排除匹配指定正则表达式的路径
 *
 * <p>这是 {@link RegexPathFilter} 的排除语义版本：
 * 匹配正则的路径被拒之门外，其余路径一律通过
 * <p>典型用途是跳过临时文件或日志目录，如排除所有以 {@code .} 开头或
 * 以 {@code _} 开头的 Hadoop 隐藏文件
 */
public class RegexExcludePathFilter implements PathFilter {
  
  /** 匹配即排除的正则表达式 */
  private final String regex;

  /**
   * 构造排除过滤器
   *
   * @param regex 匹配即排除的正则表达式
   */
  public RegexExcludePathFilter(String regex) {
    this.regex = regex;
  }

  /**
   * 判断路径是否应被排除
   *
   * @param path 待判断的路径
   * @return 路径不匹配正则时返回 true（即保留该路径）
   */
  public boolean accept(Path path) {
    return !path.toString().matches(regex);
  }
}
// ^^ RegexExcludePathFilter
