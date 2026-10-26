package crunch;

import java.io.Serializable;
import org.apache.hadoop.io.Text;

// Serializable copy of NcdcStationMetadataParser
/**
 * 解析 NCDC 气象站元数据
 *
 * <p>元数据文件里一行是一个气象站：
 * 开头是 USAF 号和 WBAN 号（中间隔一个空格），
 * 后面是站名
 * <p>这个类是给 Crunch 用的 {@code Serializable} 副本
 */
public class NcdcStationMetadataParser implements Serializable {
  
  // 站号，由 USAF 和 WBAN 两段拼成
  private String stationId;
  // 站名
  private String stationName;

  /**
   * 解析一行气象站元数据
   *
   * @param record 一行原始记录
   * @return 解析成功返回 {@code true}；表头之类的行返回 {@code false}
   */
  public boolean parse(String record) {
    // 长度不够，说明是表头行而不是数据行
    if (record.length() < 42) { // header
      // 告诉调用方这行不能用
      return false;
    }
    // USAF 号：前 6 个字符
    String usaf = record.substring(0, 6);
    // WBAN 号：第 8 到第 12 个字符
    String wban = record.substring(7, 12);
    // 拼成和其他地方一致的站号格式
    stationId = usaf + "-" + wban;
    // 站名：第 14 到第 42 个字符
    stationName = record.substring(13, 42);
    // USAF 号一定是数字，可以用解析数字的办法来验证
    try {
      // 没抛异常就说明格式对了
      Integer.parseInt(usaf); // USAF identifiers are numeric
      // 解析成功
      return true;
    // 抛异常说明 USAF 号不是数字
    } catch (NumberFormatException e) {
      // 这行不是有效的元数据
      return false;
    }
  }
  
  /**
   * 解析一行气象站元数据
   *
   * @param record Hadoop 的 {@code Text} 类型记录
   * @return 解析成功返回 {@code true}
   */
  public boolean parse(Text record) {
    // 转成字符串再交给另一个重载处理
    return parse(record.toString());
  }
  
  /**
   * 取出站号
   *
   * @return 站号
   */
  public String getStationId() {
    return stationId;
  }

  /**
   * 取出站名
   *
   * @return 站名
   */
  public String getStationName() {
    return stationName;
  }
  
}
