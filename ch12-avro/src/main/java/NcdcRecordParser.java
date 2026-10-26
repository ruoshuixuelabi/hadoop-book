import java.text.*;
import java.util.Date;

import org.apache.hadoop.io.Text;

/**
 * 解析 NCDC 格式的定宽天气记录
 *
 * <p>NCDC 的记录每行都是定长的：
 * 站号、年月日、气温、质量位各占固定的列
 * <p>本类把一行文本拆成各个字段，
 * 并判断气温读数是有效、缺测还是格式可疑
 */
public class NcdcRecordParser {
  
  // 缺测的温度用 9999 表示，这是 NCDC 数据的约定
  private static final int MISSING_TEMPERATURE = 9999;
  
  // 观测时间的格式：年、月、日、时、分
  private static final DateFormat DATE_FORMAT =
    new SimpleDateFormat("yyyyMMddHHmm");
  
  // 下面这些字段是解析一行记录后得到的结果
  private String stationId;
  private String observationDateString;
  private String year;
  private String airTemperatureString;
  private int airTemperature;
  private boolean airTemperatureMalformed;
  private String quality;
  
  /**
   * 解析一行 NCDC 记录
   *
   * <p>按固定的列位置切出站号、时间、年份、气温与质量位，
   * 结果保存在本对象的各个字段里
   *
   * @param record 一行定宽的 NCDC 记录
   */
  public void parse(String record) {
    // 站号分成两段，中间补一个连字符
    stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
    // 观测时间：年月日时分
    observationDateString = record.substring(15, 27);
    // 年份取自观测时间的前四位
    year = record.substring(15, 19);
    // 先假设气温是正常的
    airTemperatureMalformed = false;
    // 气温前面带正号的情况
    // Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
    if (record.charAt(87) == '+') {
      // 跳过正号，取后面四位数字
      airTemperatureString = record.substring(88, 92);
      // 转成整数
      airTemperature = Integer.parseInt(airTemperatureString);
    // 气温前面是负号的情况
    } else if (record.charAt(87) == '-') {
      // 负号要一起取出来，它是数值的一部分
      airTemperatureString = record.substring(87, 92);
      // 转成整数
      airTemperature = Integer.parseInt(airTemperatureString);
    } else {
      // 格式不对：既不是正号也不是负号
      airTemperatureMalformed = true;
    }
    airTemperature = Integer.parseInt(airTemperatureString);
    // 质量位：一位数字，说明这次读数的可信程度
    quality = record.substring(92, 93);
  }
  
  /**
   * 解析一行 NCDC 记录
   *
   * @param record 一行定宽的 NCDC 记录
   */
  public void parse(Text record) {
    // 转成字符串后交给另一个重载处理
    parse(record.toString());
  }
  
  /**
   * 判断气温读数是否可以放心使用
   *
   * <p>三个条件都要满足：格式没坏、不是缺测值、质量位属于可信的那几种
   *
   * @return 读数有效时返回 true
   */
  public boolean isValidTemperature() {
    // 质量位只接受 0、1、4、5、9 这几种可信的取值
    return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
        && quality.matches("[01459]");
  }
  
  /**
   * 判断气温字段的格式是否有问题
   *
   * @return 格式有问题时返回 true
   */
  public boolean isMalformedTemperature() {
    // 直接返回解析时记下的标记
    return airTemperatureMalformed;
  }
  
  /**
   * 判断气温是否缺测
   *
   * @return 缺测时返回 true
   */
  public boolean isMissingTemperature() {
    // 与约定的缺测值比较
    return airTemperature == MISSING_TEMPERATURE;
  }
  
  /**
   * 取站号
   *
   * @return 站号
   */
  public String getStationId() {
    // 解析时填好的站号
    return stationId;
  }
  
  /**
   * 取观测时间
   *
   * @return 观测时间
   * @throws IllegalArgumentException 时间格式无法解析时抛出
   */
  public Date getObservationDate() {
    try {
      // 打印原始的时间字符串，方便排查问题
      System.out.println(observationDateString);
      // 按约定的格式解析
      return DATE_FORMAT.parse(observationDateString);
    } catch (ParseException e) {
      // 时间格式有问题，直接报错
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * 取年份
   *
   * @return 年份（字符串形式）
   */
  public String getYear() {
    // 解析时填好的年份
    return year;
  }

  /**
   * 取年份
   *
   * @return 年份（整数形式）
   */
  public int getYearInt() {
    // 转成整数，方便按年份分组
    return Integer.parseInt(year);
  }

  /**
   * 取气温
   *
   * @return 气温
   */
  public int getAirTemperature() {
    // 解析时填好的气温
    return airTemperature;
  }
  
  /**
   * 取气温的原始文本
   *
   * @return 气温的原始文本
   */
  public String getAirTemperatureString() {
    // 保留原始写法，便于排查异常记录
    return airTemperatureString;
  }

  /**
   * 取质量位
   *
   * @return 质量位
   */
  public String getQuality() {
    // 解析时填好的质量位
    return quality;
  }

}
