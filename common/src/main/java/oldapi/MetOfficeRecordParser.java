package oldapi;

import java.math.*;
import org.apache.hadoop.io.Text;

/**
 * 英国气象局（Met Office）数据记录解析器（旧 API 版本）
 *
 * 功能说明：
 * - 解析英国气象局格式的天气数据记录
 * - 提取观测年份和气温值
 * - 验证气温数据的有效性
 *
 * Met Office 数据格式特点：
 * - 每条记录为文本行，字段以空格分隔
 * - 第 4-7 列：年份（4 位数字）
 * - 第 14-18 列：气温值（可能为负数或缺失标记 "---"）
 * - 气温值为摄氏度，精度为 0.1 度
 *
 * 此版本使用 org.apache.hadoop.io.Text 类型，与新 API 版本功能相同。
 *
 * @since 4.0
 */
public class MetOfficeRecordParser {

  /** 观测年份（字符串形式） */
  private String year;

  /** 气温原始字符串（解析前） */
  private String airTemperatureString;

  /** 气温整数值（摄氏度 * 10，即精度为 0.1 度） */
  private int airTemperature;

  /** 气温数据是否有效 */
  private boolean airTemperatureValid;

  /**
   * 解析字符串格式的 Met Office 记录
   *
   * @param record 原始气象数据行（空格分隔的文本）
   */
  public void parse(String record) {
    // 记录长度检查：至少需要 18 个字符才能包含有效数据
    if (record.length() < 18) {
      return;
    }

    // 解析年份：第 4-7 列（索引 3-7，不包含索引 7）
    year = record.substring(3, 7);

    // 验证年份是否为有效数字，然后解析气温
    if (isValidRecord(year)) {
      // 解析气温字符串：第 14-18 列
      airTemperatureString = record.substring(13, 18);

      // 检查是否为缺失温度（"---" 表示数据缺失）
      if (!airTemperatureString.trim().equals("---")) {
        // 将字符串转换为 BigDecimal 以精确处理小数
        BigDecimal temp = new BigDecimal(airTemperatureString.trim());
        // 乘以 10：将小数温度转换为整数表示（精度 0.1 度）
        // 例如：7.5 -> 75（表示 7.5 摄氏度）
        temp = temp.multiply(new BigDecimal(BigInteger.TEN));
        // 转换为整数（BigDecimal.intValueExact 会验证无小数部分）
        airTemperature = temp.intValueExact();
        airTemperatureValid = true;
      }
    }
  }

  /**
   * 验证年份字符串是否为有效的数字年份
   *
   * @param year 年份字符串（4 位数字）
   * @return true 表示年份格式有效
   */
  private boolean isValidRecord(String year) {
    try {
      // 尝试解析为整数，如果失败则说明格式不正确
      Integer.parseInt(year);
      return true;
    } catch (NumberFormatException e) {
      // 年份中包含非数字字符（如表头文本），视为无效记录
      return false;
    }
  }

  /**
   * 重载解析方法：支持 Hadoop Text 类型
   *
   * @param record Hadoop Text 对象
   */
  public void parse(Text record) {
    // 将 Text 转换为 String 后调用主解析方法
    parse(record.toString());
  }

  /**
   * 获取观测年份
   *
   * @return 4 位年份字符串，如 "1978"
   */
  public String getYear() {
    return year;
  }

  /**
   * 获取气温值
   *
   * @return 气温值（摄氏度 * 10，精度 0.1 度）
   *         例如：75 表示 7.5 摄氏度，-175 表示 -17.5 摄氏度
   */
  public int getAirTemperature() {
    return airTemperature;
  }

  /**
   * 获取气温原始字符串
   *
   * @return 气温值字符串，如 "  7.5" 或 "-17.5"
   */
  public String getAirTemperatureString() {
    return airTemperatureString;
  }

  /**
   * 判断气温是否有效
   *
   * 有效条件：气温值不是缺失标记（---）
   *
   * @return true 表示气温数据有效
   */
  public boolean isValidTemperature() {
    return airTemperatureValid;
  }

}
