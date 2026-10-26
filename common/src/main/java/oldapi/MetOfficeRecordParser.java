package oldapi;

import java.math.*;
import org.apache.hadoop.io.Text;

/**
 * 解析英国气象局（Met Office）的气象记录（老 API 版）
 *
 * <p>这份是 {@code common} 模块里给「旧版 {@code mapred} API」准备的副本，
 * 放在 {@code oldapi} 包下，逻辑和新 API 版完全一样
 * <p>Met Office 的原始数据和 NCDC 的格式不一样：
 * 温度是以摄氏度表示的小数，而且整个文件前面还有若干行表头
 * <p>这里把温度乘以 10 转成整数，
 * 这样丢进 MapReduce 时就不用处理浮点数了
 */
public class MetOfficeRecordParser {
  
  // 年份
  private String year;
  // 温度字段的原始字符串
  private String airTemperatureString;
  // 气温乘以 10 之后的整数
  private int airTemperature;
  // 这一行的温度是不是有效的
  private boolean airTemperatureValid;
  
  /**
   * 解析一行 Met Office 记录
   *
   * @param record 一行原始记录
   */
  public void parse(String record) {
    // 太短了，不是数据行（多半是表头）
    if (record.length() < 18) {
      return;
    }
    // 第 4 到第 7 位是年份
    year = record.substring(3, 7);
    // 年份是数字才说明这是一条真正的记录
    if (isValidRecord(year)) {
      // 第 14 到第 18 位是温度
      airTemperatureString = record.substring(13, 18);
      // 温度字段是 ---，表示温度缺失
      if (!airTemperatureString.trim().equals("---")) {
        // 用 BigDecimal 是为了精确处理小数
        BigDecimal temp = new BigDecimal(airTemperatureString.trim());
        // 乘以 10，把小数变成整数
        temp = temp.multiply(new BigDecimal(BigInteger.TEN));
        // 取整；万一不是整数会抛异常
        airTemperature = temp.intValueExact();
        // 标记这一行温度有效
        airTemperatureValid = true;
      }
    }
  }
  
  /**
   * 判断年份是不是数字
   *
   * @param year 年份字符串
   * @return 是数字返回 {@code true}
   */
  private boolean isValidRecord(String year) {
    try {
      // 能解析成数字就说明是数据行
      Integer.parseInt(year);
      // 解析成功
      return true;
    } catch (NumberFormatException e) {
      // 年份不是数字，说明这一行是表头之类的行
      return false;
    }
  }

  /**
   * 解析一行 Met Office 记录（{@link Text} 版本）
   *
   * @param record 一行原始记录
   */
  public void parse(Text record) {
    // 转成字符串，复用字符串版本
    parse(record.toString());
  }
  
  /**
   * 取年份
   *
   * @return 年份
   */
  public String getYear() {
    return year;
  }

  /**
   * 取气温
   *
   * @return 乘以 10 之后的整数温度
   */
  public int getAirTemperature() {
    return airTemperature;
  }
  
  /**
   * 取温度字段的原始字符串
   *
   * @return 温度字段的原始字符串
   */
  public String getAirTemperatureString() {
    return airTemperatureString;
  }

  /**
   * 判断这一行的温度能不能用
   *
   * @return 有效返回 {@code true}
   */
  public boolean isValidTemperature() {
    return airTemperatureValid;
  }

}
