// cc RegexExcludePathFilter A PathFilter for excluding paths that match a regular expression

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

// vv RegexExcludePathFilter
/**
 * 基于正则表达式的排除型PathFilter实现类。
 *
 * <p>此类是RegexPathFilter的简化版本，专门用于排除匹配正则表达式的路径。
 * 与RegexPathFilter( regex, false ) 等价，但使用更直观。</p>
 *
 * <p>典型应用场景：</p>
 * <ul>
 *   <li>排除特定模式的文件（如所有以.tmp结尾的临时文件）</li>
 *   <li>排除特定日期的目录（如排除2007年的所有数据）</li>
 *   <li>过滤掉不需要处理的文件</li>
 * </ul>
 *
 * <p>使用示例：</p>
 *
 *   // 排除所有2007开头的路径
 *
 *
 * @see PathFilter Hadoop路径过滤器接口
 * @see RegexPathFilter 支持包含和排除两种模式的正则过滤器
 */
public class RegexExcludePathFilter implements PathFilter {

  // 存储正则表达式字符串
  private final String regex;

  /**
   * 构造一个排除型正则过滤器。
   *
   * @param regex Java正则表达式字符串，匹配此表达式的路径将被排除
   */
  public RegexExcludePathFilter(String regex) {
    this.regex = regex;
  }

  /**
   * 判断路径是否应该被排除。
   *
   * <p>逻辑：匹配正则表达式的路径返回false（排除），
   * 不匹配的路径返回true（接受）。</p>
   *
   * @param path 要检查的HDFS路径
   * @return 不匹配正则表达式则接受，匹配则排除
   */
  public boolean accept(Path path) {
    // 注意：这里取反！即匹配regex的路径返回false（被排除）
    return !path.toString().matches(regex);
  }
}
// ^^ RegexExcludePathFilter
