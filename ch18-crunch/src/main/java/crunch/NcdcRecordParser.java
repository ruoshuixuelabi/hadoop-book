package crunch;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.Text;

// Serializable copy of NcdcRecordParser
/**
 * 解析 NCDC 气象记录
 *
 * <p>NCDC 的记录是定长格式：
 * 每个字段占固定的字符数，靠位置而不是靠分隔符来区分
 * <p>这个类是第 2 章 {@code NcdcRecordParser} 的 {@code Serializable} 副本，
 * 专门给 Crunch 的 {@code DoFn} 用——
 * {@code DoFn} 要能被序列化后分发到各个任务里，
 * 所以解析器不能持有不可序列化的状态
 */
public class NcdcRecordParser implements Serializable {
  
  // NCDC 用 9999 表示温度缺失
  private static final int MISSING_TEMPERATURE = 9999;
  
  // 观测时间的格式：年月日时分
  private static final DateFormat DATE_FORMAT =
    new SimpleDateFormat("yyyyMMddHHmm");
  
  // 站号，由 USAF 和 WBAN 两段拼成
  private String stationId;
  // 观测时间的原始字符串
  private String observationDateString;
  // 观测年份
  private String year;
  // 气温的原始字符串
  private String airTemperatureString;
  // 气温
  private int airTemperature;
  // 气温字段是否格式错误
  private boolean airTemperatureMalformed;
  // 质量码
  private String quality;
  
  /**
   * 解析一行 NCDC 记录
   *
   * <p>结果写进本对象的各个字段里
   *
   * @param record 一行原始记录
   */
  public void parse(String record) {
    // 站号 = USAF 段（第 5 到第 10 个字符）+ "-" + WBAN 段（第 11 到第 15 个字符）
    stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
    // 观测时间：第 16 到第 27 个字符
    observationDateString = record.substring(15, 27);
    // 年份就是观测时间的前 4 位
    year = record.substring(15, 19);
    // 先假设格式没问题，下面再判断
    airTemperatureMalformed = false;
    // Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
    // 补充：Java 7 之前 Integer.parseInt 不接受开头的 "+"
    // 所以正数要把正号那一格单独跳过
    // 温度字段以正号开头
    if (record.charAt(87) == '+') {
      // 跳过正号本身，取后面 4 位
      airTemperatureString = record.substring(88, 92);
      // 转成整数
      airTemperature = Integer.parseInt(airTemperatureString);
    // 温度字段以负号开头
    } else if (record.charAt(87) == '-') {
      // 负号要留着，取 5 位
      airTemperatureString = record.substring(87, 92);
      // 转成整数
      airTemperature = Integer.parseInt(airTemperatureString);
    // 既不是正号也不是负号，说明这一格坏了
    } else {
      // 记下「格式错误」，但这一行的温度字符串仍然是 null
      airTemperatureMalformed = true;
    }
    // 注意：这一行在 if/else 之外无条件执行，是原书示例的原样写法
    // 所以当上面的 else 分支被走到时，这里会因为字符串是 null 而抛异常
    airTemperature = Integer.parseInt(airTemperatureString);
    // 质量码：第 93 个字符
    quality = record.substring(92, 93);
  }
  
  /**
   * 解析一行 NCDC 记录
   *
   * @param record Hadoop 的 {@code Text} 类型记录
   */
  public void parse(Text record) {
    // 转成字符串再交给另一个重载处理
    parse(record.toString());
  }
  
  /**
   * 判断气温是否可以拿来用
   *
   * <p>三个条件缺一不可：
   * 格式没坏、不是缺失值、质量码在允许的集合里
   *
   * @return 可用返回 {@code true}
   */
  public boolean isValidTemperature() {
    // 质量码只认 0、1、4、5、9 这几个「好」的
    return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
        && quality.matches("[01459]");
  }
  
  /**
   * 判断气温字段是否格式错误
   *
   * @return 格式错误返回 {@code true}
   */
  public boolean isMalformedTemperature() {
    // 之前 parse 时记下的标志
    return airTemperatureMalformed;
  }
  
  /**
   * 判断气温是否缺失
   *
   * @return 缺失返回 {@code true}
   */
  public boolean isMissingTemperature() {
    // 和 NCDC 的缺失标记 9999 比一下
    return airTemperature == MISSING_TEMPERATURE;
  }
  
  /**
   * 取出站号
   *
   * @return 站号，形如 {@code 724666-94846}
   */
  public String getStationId() {
    return stationId;
  }
  
  /**
   * 取出观测时间
   *
   * @return 观测时间
   * @throws IllegalArgumentException 时间格式无法解析时抛出
   */
  public Date getObservationDate() {
    // 按约定的格式解析时间字符串
    try {
      // 调试输出：把原始时间串打出来
      System.out.println(observationDateString);
      // 解析成 Date 对象
      return DATE_FORMAT.parse(observationDateString);
    // 解析失败说明数据本身有问题
    } catch (ParseException e) {
      // 这里选择直接抛出去，而不是返回一个错误的日期
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * 取出年份
   *
   * @return 年份字符串
   */
  public String getYear() {
    return year;
  }

  /**
   * 取出年份
   *
   * <p>和 {@link #getYear()} 的区别是这个返回整数，
   * 排序和分组的时候用整数更方便
   *
   * @return 年份
   */
  public int getYearInt() {
    // 年份本来就是数字，转换不会出问题
    return Integer.parseInt(year);
  }

  /**
   * 取出气温
   *
   * @return 气温
   */
  public int getAirTemperature() {
    return airTemperature;
  }
  
  /**
   * 取出气温的原始字符串
   *
   * @return 原始的气温文本
   */
  public String getAirTemperatureString() {
    return airTemperatureString;
  }

  /**
   * 取出质量码
   *
   * @return 质量码
   */
  public String getQuality() {
    return quality;
  }

}
