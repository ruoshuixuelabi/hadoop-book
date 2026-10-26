package crunch;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.hadoop.io.Text;

// Serializable copy of MetOfficeRecordParser
/**
 * 解析 Met Office 气象记录
 *
 * <p>Met Office 的记录长这样：
 * {@code 2000,01,01,0,12.3}，字段之间用逗号分隔
 * <p>和 {@link NcdcRecordParser} 一样，
 * 这个类被做成了 {@code Serializable} 的副本，
 * 好让 Crunch 的 {@code DoFn} 能把它当字段带着跑
 */
public class MetOfficeRecordParser implements Serializable {
  
  // 观测年份，字符串形式
  private String year;
  // 气温的原始字符串（可能是 "---"，表示缺失）
  private String airTemperatureString;
  // 气温（已乘以 10，转成整数）
  private int airTemperature;
  // 气温是否有效
  private boolean airTemperatureValid;
  
  /**
   * 解析一行 Met Office 记录
   *
   * <p>解析结果写进本对象的各个字段里，
   * 不会返回什么
   *
   * @param record 一行原始记录
   */
  public void parse(String record) {
    // 记录太短，说明不是有效的数据行
    if (record.length() < 18) {
      // 直接放弃，不更新任何字段
      return;
    }
    // 年份在第 4 到第 7 个字符
    year = record.substring(3, 7);
    // 年份不是数字就说明这行有问题
    if (isValidRecord(year)) {
      // 气温在第 14 到第 18 个字符
      airTemperatureString = record.substring(13, 18);
      // "---" 表示这个位置没有数据
      if (!airTemperatureString.trim().equals("---")) {
        // 先转成 BigDecimal，后面要乘以 10
        BigDecimal temp = new BigDecimal(airTemperatureString.trim());
        // 乘以 10 去掉小数点
        // 用 BigDecimal 是为了不丢精度
        temp = temp.multiply(new BigDecimal(BigInteger.TEN));
        // 再转回整数；intValueExact 在有小数时会抛异常
        airTemperature = temp.intValueExact();
        // 标记气温有效
        airTemperatureValid = true;
      }
    }
  }
  
  /**
   * 判断年份字段是不是一个合法的数字
   *
   * @param year 年份字符串
   * @return 是数字就返回 {@code true}
   */
  private boolean isValidRecord(String year) {
    // 用解析数字的办法来试探一下
    try {
      // 没抛异常就说明是数字
      Integer.parseInt(year);
      // 是合法记录
      return true;
    // 抛异常说明不是数字
    } catch (NumberFormatException e) {
      // 不是合法记录
      return false;
    }
  }

  /**
   * 解析一行 Met Office 记录
   *
   * @param record Hadoop 的 {@code Text} 类型记录
   */
  public void parse(Text record) {
    // 转成字符串再交给另一个重载处理
    parse(record.toString());
  }
  
  /**
   * 取出解析出来的年份
   *
   * @return 年份字符串
   */
  public String getYear() {
    return year;
  }

  /**
   * 取出解析出来的气温
   *
   * @return 乘以 10 之后的整数气温
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
   * 判断气温是否有效
   *
   * @return 有效返回 {@code true}
   */
  public boolean isValidTemperature() {
    return airTemperatureValid;
  }

}
