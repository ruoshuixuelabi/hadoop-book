import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 按日期区间筛选路径的过滤器
 *
 * <p>用于 {@code FileSystem.glob} 与 {@code listStatus(Path, PathFilter)}
 * 的路径过滤：从形如 {@code /2007/12/30} 的目录结构中，
 * 只保留落在指定起止日期（含端点）内的路径
 * <p>典型场景是「只处理某段时间内的日志目录」
 */
public class DateRangePathFilter implements PathFilter {
  
  /**
   * 匹配路径中的日期部分：形如 {@code 2007/12/30} 的三段数字
   * <p>用捕获组把日期部分单独取出，供后续解析
   */
  private final Pattern PATTERN = Pattern.compile("^.*/(\\d\\d\\d\\d/\\d\\d/\\d\\d).*$");
  
  /** 区间起止日期（含两端），由构造参数传入 */
  private final Date start, end;

  /**
   * 构造日期区间过滤器
   *
   * @param start 区间起始日期
   * @param end   区间结束日期
   */
  public DateRangePathFilter(Date start, Date end) {
    // 复制一份 Date 而非直接持有引用：Date 是可变对象，
    // 直接持有会让调用方后续的修改影响到已构造好的过滤器
    this.start = new Date(start.getTime());
    this.end = new Date(end.getTime());
  }
  
  /**
   * 判断路径是否落在日期区间内
   *
   * @param path 待判断的路径
   * @return 路径含可解析的日期且该日期在区间内时返回 true
   */
  public boolean accept(Path path) {
    Matcher matcher = PATTERN.matcher(path.toString());
    // 只有形如 "年/月/日" 结构的路径才可能命中日期区间
    if (matcher.matches()) {
      DateFormat format = new SimpleDateFormat("yyyy/MM/dd");
      try {
        return inInterval(format.parse(matcher.group(1)));
      } catch (ParseException e) {
        // 日期虽然匹配了格式但无法解析成合法日期（如 2007/13/45），按不通过处理
        return false;
      }
    }
    return false;
  }

  /**
   * 判断日期是否落在闭区间 [start, end] 内
   *
   * @param date 待判断的日期
   * @return 在区间内（含两端）返回 true
   */
  private boolean inInterval(Date date) {
    // 用「不早于 start 且不晚于 end」表达闭区间，直接比较 Date 的时间戳
    return !date.before(start) && !date.after(end);
  }

}
