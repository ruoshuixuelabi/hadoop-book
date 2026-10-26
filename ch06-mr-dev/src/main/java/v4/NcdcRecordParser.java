package v4;
import org.apache.hadoop.io.Text;

/**
 * 能识别「温度字段格式损坏」的 NCDC 记录解析器
 *
 * <p>与 v2 相比，多了一个 {@code airTemperatureMalformed} 标志：
 * 当温度位置上的字符既不是 '+' 也不是 '-' 时，
 * 说明这条记录是坏的，而不是温度恰好缺失
 * <p>有了这个区分，Mapper 才能只对真正的坏数据报警
 */
public class NcdcRecordParser {
  
  // 9999 表示「温度缺失」——这是合法值，不是错误
  private static final int MISSING_TEMPERATURE = 9999;
  
  // 解析出来的年份
  private String year;
  // 解析出来的气温；记录损坏时这个值没有意义
  private int airTemperature;
  // 温度字段是否格式损坏——与「温度缺失」是两回事
  private boolean airTemperatureMalformed;
  // 质量标记
  private String quality;
  
  /**
   * 解析一条记录，并判断温度字段是否损坏
   *
   * @param record 一整行记录
   */
  public void parse(String record) {
    // 年份占第 15~19 位
    year = record.substring(15, 19);
    // 先假定没坏，解析时再按情况推翻
    airTemperatureMalformed = false;
    // Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
    // 温度带正号：跳过符号位，温度实际占第 88~91 位
    //（Java 7 之前 parseInt 不接受正号，所以必须先切掉符号）
    if (record.charAt(87) == '+') { 
      airTemperature = Integer.parseInt(record.substring(88, 92));
    // 温度带负号：符号和数字一起占第 87~91 位
    } else if (record.charAt(87) == '-') {
      airTemperature = Integer.parseInt(record.substring(87, 92));
    // 温度位置既不是 '+' 也不是 '-'：这条记录的格式坏了
    } else {
      airTemperatureMalformed = true;
    }
    // 质量标记只有一位
    quality = record.substring(92, 93);
  }
  
  /**
   * 解析一条 Text 类型的记录
   *
   * @param record 一整行记录
   */
  public void parse(Text record) {
    // 真正干活的是 String 版本，这里只做一次类型转换
    parse(record.toString());
  }

  /**
   * 判断温度是否既存在又可信
   *
   * @return 没坏、不是缺失值、且质量标记可信时返回 true
   */
  public boolean isValidTemperature() {
    // 三个条件：格式没坏、不是 9999（缺失）、质量位属于 {0,1,4,5,9}
    return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
        && quality.matches("[01459]");
  }
  
  /**
   * 判断温度字段是否格式损坏
   *
   * @return 格式损坏时返回 true
   */
  public boolean isMalformedTemperature() {
    return airTemperatureMalformed;
  }
  
  /**
   * 取得解析出的年份
   *
   * @return 年份字符串
   */
  public String getYear() {
    return year;
  }

  /**
   * 取得解析出的气温
   *
   * @return 气温
   */
  public int getAirTemperature() {
    return airTemperature;
  }
}
