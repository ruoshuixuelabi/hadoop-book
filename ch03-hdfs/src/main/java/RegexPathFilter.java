import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 按正则表达式筛选路径的过滤器
 *
 * <p>用「匹配则保留 / 匹配则排除」两种模式覆盖 glob 的常见需求：
 * {@code *} 展开全部、{@code ?} 展开单字符、
 * {@code [a-z]} 展开字符集、{@code {a,b}} 展开多个模式
 * <p>也可用于实现排除语义（把 include 设为 false）
 */
public class RegexPathFilter implements PathFilter {
  
  /** 用于匹配路径字符串的正则表达式 */
  private final String regex;

  /** 匹配时是否保留该路径；为 false 表示「匹配则排除」 */
  private final boolean include;

  /**
   * 构造匹配即保留的过滤器
   *
   * @param regex 路径匹配用的正则表达式
   */
  public RegexPathFilter(String regex) {
    this(regex, true);
  }
  
  /**
   * 构造过滤器并指定匹配后的处理方式
   *
   * @param regex  路径匹配用的正则表达式
   * @param include 为 true 时「匹配则保留」，为 false 时「匹配则排除」
   */
  public RegexPathFilter(String regex, boolean include) {
    this.regex = regex;
    this.include = include;
  }

  /**
   * 判断路径是否符合过滤条件
   *
   * @param path 待判断的路径
   * @return 匹配结果与 include 一致时返回 true
   */
  public boolean accept(Path path) {
    // 匹配则按 include 决定去留，不匹配则取反，两种语义共用一次匹配
    return (path.toString().matches(regex)) ? include : !include;
  }

}
