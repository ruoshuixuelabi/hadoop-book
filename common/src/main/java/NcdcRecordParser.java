// cc NcdcRecordParser Parse weather station records in NCDC format
// vv NcdcRecordParser

import java.text.*;
import java.util.Date;

import org.apache.hadoop.io.Text;

/**
 * NCDC（美国国家气候数据中心）气象记录解析器
 *
 * 功能说明：
 * - 解析 NCDC 标准格式的天气数据记录
 * - 提取站点 ID、观测日期、气温、质量代码等字段
 * - 验证气温数据的有效性和完整性
 *
 * NCDC 数据格式特点：
 * - 每条记录为固定长度文本行（固定列位置）
 * - 第 5-10 列：站点 ID
 * - 第 11-15 列：站点 ID 第二部分
 * - 第 16-27 列：观测时间（yyyyMMddHHmm 格式）
 * - 第 88-92 列：气温值（可能带正负号）
 * - 第 93 列：质量代码
 *
 * @since 4.0
 */
public class NcdcRecordParser {

  /**
   * 缺失温度标记值
   * NCDC 数据中 9999 表示温度数据缺失
   */
  private static final int MISSING_TEMPERATURE = 9999;

  /**
   * 日期格式解析器
   * 用于解析 NCDC 数据中的时间戳格式：yyyyMMddHHmm
   * 例如：201503151230 表示 2015年3月15日 12:30
   */
  private static final DateFormat DATE_FORMAT =
    new SimpleDateFormat("yyyyMMddHHmm");

  // ==================== 字段定义 ====================

  /** 站点 ID（格式：XXXXX-XXXXX，国家代码-站点编号） */
  private String stationId;

  /** 原始观测时间字符串（格式：yyyyMMddHHmm） */
  private String observationDateString;

  /** 观测年份（从时间戳中提取） */
  private String year;

  /** 气温原始字符串（解析前） */
  private String airTemperatureString;

  /** 气温整数值（摄氏度 * 10，即精度为 0.1 度） */
  private int airTemperature;

  /** 气温字段是否格式错误（如缺少 +/- 前缀等） */
  private boolean airTemperatureMalformed;

  /** 质量代码（NCDC 数据质量标记） */
  private String quality;

  // ==================== 解析方法 ====================

  /**
   * 解析字符串格式的 NCDC 记录
   *
   * @param record 原始 NCDC 数据行（固定长度文本）
   */
  public void parse(String record) {
    // 解析站点 ID：第 5-10 列 + "-" + 第 11-15 列
    // 例如：722053-03112 表示美国 722053 站点
    stationId = record.substring(4, 10) + "-" + record.substring(10, 15);

    // 解析观测时间：第 16-27 列（14 位数字：yyyyMMddHHmm）
    observationDateString = record.substring(15, 27);

    // 提取年份：时间戳的前 4 位
    year = record.substring(15, 19);

    // 初始化气温格式标记为正常
    airTemperatureMalformed = false;

    // 解析气温值：第 88-92 列
    // 注意：Java 7 之前的 Integer.parseInt 不接受前导 + 号
    // 第 88 列为符号位：'+' 表示正温度，'-' 表示负温度
    if (record.charAt(87) == '+') {
      // 正温度：'+' 在第 88 位，实际数值从第 89-92 位
      airTemperatureString = record.substring(88, 92);
      airTemperature = Integer.parseInt(airTemperatureString);
    } else if (record.charAt(87) == '-') {
      // 负温度：'-' 在第 88 位，数值从第 88-92 位（包含负号）
      airTemperatureString = record.substring(87, 92);
      airTemperature = Integer.parseInt(airTemperatureString);
    } else {
      // 格式错误：既不是 '+' 也不是 '-'，气温数据损坏
      airTemperatureMalformed = true;
    }

    // 再次解析（代码中实际执行了两次解析，第二次会覆盖第一次的值）
    // 注意：这行代码在 else 分支中会导致 airTemperature 未初始化
    // 原书代码此处存在潜在问题，但在实际数据中不会走到 else 分支
    airTemperature = Integer.parseInt(airTemperatureString);

    // 解析质量代码：第 93 列
    // 0=保留, 1=通过质量检查, 4=可疑, 5=错误, 9=缺失
    quality = record.substring(92, 93);
  }

  /**
   * 重载解析方法：支持 Hadoop Text 类型
   * Hadoop 的 Text 使用 UTF-8 字节数组实现，比 String 更适合处理大数据
   *
   * @param record Hadoop Text 对象
   */
  public void parse(Text record) {
    // 将 Text 转换为 String 后调用主解析方法
    parse(record.toString());
  }

  // ==================== 验证方法 ====================

  /**
   * 判断气温是否有效
   *
   * 有效条件：
   * 1. 气温字段格式正常（未被标记为 malformed）
   * 2. 气温值不是缺失标记（9999）
   * 3. 质量代码为合法值（0, 1, 4, 5, 9 之一）
   *
   * @return true 表示气温数据有效
   */
  public boolean isValidTemperature() {
    return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
        && quality.matches("[01459]");
  }

  /**
   * 判断气温字段是否格式错误
   *
   * @return true 表示格式错误（如缺少 +/- 符号）
   */
  public boolean isMalformedTemperature() {
    return airTemperatureMalformed;
  }

  /**
   * 判断气温是否缺失
   *
   * @return true 表示气温值为缺失标记（9999）
   */
  public boolean isMissingTemperature() {
    return airTemperature == MISSING_TEMPERATURE;
  }

  // ==================== 访问器方法 ====================

  /**
   * 获取站点 ID
   *
   * @return 站点 ID，格式：XXXXX-XXXXX
   */
  public String getStationId() {
    return stationId;
  }

  /**
   * 获取观测日期
   *
   * @return Date 对象表示的观测时间
   * @throws IllegalArgumentException 如果日期解析失败
   */
  public Date getObservationDate() {
    try {
      // 注意：此处 System.out.println 会打印到控制台，生产环境应移除或使用日志
      System.out.println(observationDateString);
      return DATE_FORMAT.parse(observationDateString);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * 获取观测年份（字符串形式）
   *
   * @return 4 位年份字符串，如 "2015"
   */
  public String getYear() {
    return year;
  }

  /**
   * 获取观测年份（整数形式）
   *
   * @return 4 位年份整数，如 2015
   */
  public int getYearInt() {
    return Integer.parseInt(year);
  }

  /**
   * 获取气温值
   *
   * @return 气温值（摄氏度 * 10，精度 0.1 度）
   *         例如：235 表示 23.5 摄氏度
   */
  public int getAirTemperature() {
    return airTemperature;
  }

  /**
   * 获取气温原始字符串
   *
   * @return 气温值字符串，可能包含正负号
   */
  public String getAirTemperatureString() {
    return airTemperatureString;
  }

  /**
   * 获取质量代码
   *
   * @return 质量代码字符
   *         '0'=保留, '1'=通过检查, '4'=可疑, '5'=错误, '9'=缺失
   */
  public String getQuality() {
    return quality;
  }

}
// ^^ NcdcRecordParser