package oldapi;

import org.apache.hadoop.io.Text;

/**
 * NCDC 气象站元数据解析器（旧 API 版本）
 *
 * 功能说明：
 * - 解析 ISD（Integrated Surface Database）气象站历史数据行
 * - 提取站点 ID（USAF-WBAN 格式）和站点名称
 * - 验证站点 ID 的有效性
 *
 * ISD 数据格式：
 * - 第 0-5 列：USAF 站点标识符（6 位数字）
 * - 第 6 列：空格分隔符
 * - 第 7-11 列：WBAN 站点标识符（5 位数字）
 * - 第 12 列：空格分隔符
 * - 第 13-41 列：站点名称（最多 29 个字符）
 * - 其余列：国家代码、经纬度等附加信息
 *
 * 示例行：
 * 715390 99999 MOOSE JAW CS CN CA SA CZMJ +50317 -105550 +05770
 *
 * 此版本使用 org.apache.hadoop.io.Text 类型，与新 API 版本功能相同。
 *
 * @since 4.0
 */
public class NcdcStationMetadataParser {

  /** 站点 ID（格式：USAF-WBAN，如 "715390-99999"） */
  private String stationId;

  /** 站点名称（如 "MOOSE JAW CS"） */
  private String stationName;

  /**
   * 解析字符串格式的气象站元数据记录
   *
   * @param record 原始 ISD 数据行
   * @return true 表示解析成功且站点 ID 有效，false 表示跳过或解析失败
   */
  public boolean parse(String record) {
    // 长度检查：表头行通常较短，跳过（小于 42 字符）
    if (record.length() < 42) { // header
      return false;
    }

    // 提取 USAF 标识符：第 0-5 列
    String usaf = record.substring(0, 6);

    // 提取 WBAN 标识符：第 7-11 列（跳过第 6 列的空格）
    String wban = record.substring(7, 12);

    // 组合站点 ID：USAF-WBAN 格式
    stationId = usaf + "-" + wban;

    // 提取站点名称：第 13-41 列
    stationName = record.substring(13, 42);

    // 验证 USAF 标识符是否为数字（USAF 标识符都是数字）
    try {
      Integer.parseInt(usaf); // USAF identifiers are numeric
      return true;
    } catch (NumberFormatException e) {
      // 非数字格式说明不是有效的气象站记录
      return false;
    }
  }

  /**
   * 重载解析方法：支持 Hadoop Text 类型
   *
   * @param record Hadoop Text 对象
   * @return true 表示解析成功
   */
  public boolean parse(Text record) {
    return parse(record.toString());
  }

  /**
   * 获取解析后的站点 ID
   *
   * @return 站点 ID，格式：USAF-WBAN（如 "715390-99999"）
   */
  public String getStationId() {
    return stationId;
  }

  /**
   * 获取解析后的站点名称
   *
   * @return 站点名称（如 "MOOSE JAW CS"），可能包含前后空格
   */
  public String getStationName() {
    return stationName;
  }

}
