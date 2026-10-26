import org.apache.hadoop.io.Text;

/**
 * 解析 NCDC 气象站元数据文件的一行
 *
 * <p>元数据文件每一行是一个站的信息：
 * 开头 12 个字符是 USAF 号和 WBAN 号，第 13 个字符之后是站名
 * <p>文件第一行是表头，解析器会把它拒掉
 */
public class NcdcStationMetadataParser {
  
  // 站号
  private String stationId;
  // 站名
  private String stationName;

  /**
   * 解析一行元数据
   *
   * <p>站号里的 USAF 部分必须是数字，否则这一行就不是有效的站记录
   *
   * @param record 一行元数据
   * @return 解析成功返回 {@code true}，否则返回 {@code false}
   */
  public boolean parse(String record) {
    // 长度不够，说明是表头或者空行
    if (record.length() < 42) { // header
      return false;
    }
    // 头 6 位是 USAF 号
    String usaf = record.substring(0, 6);
    // 第 8 到第 12 位是 WBAN 号
    String wban = record.substring(7, 12);
    // 站号 = USAF + "-" + WBAN
    stationId = usaf + "-" + wban;
    // 第 14 位到第 42 位是站名
    stationName = record.substring(13, 42);
    try {
      // USAF 号必须是数字，不是就说明这行不对
      Integer.parseInt(usaf); // USAF identifiers are numeric
      // 解析成功
      return true;
    } catch (NumberFormatException e) {
      // USAF 号不是数字，这一行不是有效的站记录
      return false;
    }
  }
  
  /**
   * 解析一行元数据（{@link Text} 版本）
   *
   * @param record 一行元数据
   * @return 解析成功返回 {@code true}，否则返回 {@code false}
   */
  public boolean parse(Text record) {
    // 转成字符串，复用字符串版本
    return parse(record.toString());
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
   * 取站名
   *
   * @return 站名
   */
  public String getStationName() {
    return stationName;
  }
  
}
