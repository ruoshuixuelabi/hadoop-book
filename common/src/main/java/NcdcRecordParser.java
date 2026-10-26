import java.text.*;
import java.util.Date;

import org.apache.hadoop.io.Text;

/**
 * 解析 NCDC 气象记录的一行文本
 *
 * <p>NCDC 的原始记录是定长文本，每个字段都占固定的位置，
 * 所以这里全靠 {@code substring} 按下标去切
 * <p>解析出来的结果放在实例字段上，
 * 所以一个解析器实例在同一时刻只能表示一条记录
 */
public class NcdcRecordParser {
  
  // NCDC 用 9999 表示「温度缺失」，不是真的测到了这个温度
  private static final int MISSING_TEMPERATURE = 9999;
  
  // 观测时间在原始记录里的格式：yyyyMMddHHmm
  private static final DateFormat DATE_FORMAT =
    new SimpleDateFormat("yyyyMMddHHmm");
  
  // 站号，形如 011990-99999
  private String stationId;
  // 观测时间的原始字符串
  private String observationDateString;
  // 观测年份
  private String year;
  // 温度字段的原始字符串
  private String airTemperatureString;
  // 解析出来的气温
  private int airTemperature;
  // 温度字段格式不对的标记
  private boolean airTemperatureMalformed;
  // 质量标记
  private String quality;
  
  /**
   * 解析一行 NCDC 记录
   *
   * <p>按下标把各字段切出来。其中温度字段最麻烦：
   * 正数前面带一个 {@code +} 号，
   * 而 {@code Integer.parseInt} 在 Java 7 之前不接受开头的 {@code +}
   *
   * @param record 一行原始记录
   */
  public void parse(String record) {
    // 站号由 USAF 号和 WBAN 号拼成，中间用短横线连起来
    stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
    // 观测时间：年、月、日、时、分各占两位
    observationDateString = record.substring(15, 27);
    // 年份就是观测时间的前四位
    year = record.substring(15, 19);
    // 每解析一行都要先把「格式错误」的标记复位
    airTemperatureMalformed = false;
    // Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
    // 补充：Java 7 之前 Integer.parseInt 不接受开头的 "+"
    // 正数：正号那一格单独跳过，从下一位开始取
    if (record.charAt(87) == '+') {
      // 正数的温度占 4 位
      airTemperatureString = record.substring(88, 92);
      // 解析成整数
      airTemperature = Integer.parseInt(airTemperatureString);
    // 负数：连着负号一起取
    } else if (record.charAt(87) == '-') {
      // 负数的温度占 5 位
      airTemperatureString = record.substring(87, 92);
      // 解析成整数
      airTemperature = Integer.parseInt(airTemperatureString);
    // 既不是加号也不是减号，说明这一格的格式不对
    } else {
      // 记下格式错误
      airTemperatureMalformed = true;
    }
    airTemperature = Integer.parseInt(airTemperatureString);
    // 质量标记在温度字段后面一格
    quality = record.substring(92, 93);
  }
  
  /**
   * 解析一行 NCDC 记录（{@link Text} 版本）
   *
   * <p>MapReduce 里拿到的键值都是 {@link Text}，
   * 所以专门提供一个重载，转成字符串再交给上面那个方法
   *
   * @param record 一行原始记录
   */
  public void parse(Text record) {
    // 转成字符串，复用字符串版本的那套解析逻辑
    parse(record.toString());
  }
  
  /**
   * 判断这条记录的温度能不能用
   *
   * <p>三个条件都要满足：格式没错、不是缺失值、质量标记合格
   *
   * @return 可以用就返回 {@code true}
   */
  public boolean isValidTemperature() {
    // 格式没错，而且不是缺失值
    return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
        && quality.matches("[01459]");
  }
  
  /**
   * 判断温度字段的格式是不是不对
   *
   * @return 格式不对返回 {@code true}
   */
  public boolean isMalformedTemperature() {
    return airTemperatureMalformed;
  }
  
  /**
   * 判断温度是不是缺失的
   *
   * @return 缺失返回 {@code true}
   */
  public boolean isMissingTemperature() {
    return airTemperature == MISSING_TEMPERATURE;
  }
  
  /**
   * 取站号
   *
   * @return 站号
   */
  public String getStationId() {
    return stationId;
  }
  
  /**
   * 取观测时间
   *
   * @return 观测时间
   */
  public Date getObservationDate() {
    try {
      // 调试用的输出，正式代码里可以去掉
      System.out.println(observationDateString);
      // 按 yyyyMMddHHmm 解析
      return DATE_FORMAT.parse(observationDateString);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * 取观测年份
   *
   * @return 年份（字符串）
   */
  public String getYear() {
    return year;
  }

  /**
   * 取观测年份
   *
   * @return 年份（整数）
   */
  public int getYearInt() {
    return Integer.parseInt(year);
  }

  /**
   * 取气温
   *
   * @return 气温
   */
  public int getAirTemperature() {
    return airTemperature;
  }
  
  /**
   * 取温度字段的原始字符串
   *
   * <p>有些统计需要看温度的原始写法（比如判断是不是 {@code ---}）
   *
   * @return 温度字段的原始字符串
   */
  public String getAirTemperatureString() {
    return airTemperatureString;
  }

  /**
   * 取质量标记
   *
   * @return 质量标记
   */
  public String getQuality() {
    return quality;
  }

}
