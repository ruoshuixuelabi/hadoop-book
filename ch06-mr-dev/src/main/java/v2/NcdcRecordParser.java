// cc NcdcRecordParserV2 A class for parsing weather records in NCDC format
package v2;
import org.apache.hadoop.io.Text;

// vv NcdcRecordParserV2
/**
 * 解析 NCDC 格式的气象记录
 *
 * <p>NCDC 记录是定长文本，各字段位置固定：
 * 第 15~19 位是年份，第 87~91 位是气温，第 92 位是质量标记
 * <p>解析分两步：先 {@code parse} 填字段，再 {@code isValidTemperature}
 * 判断这条记录能不能用
 * <p>把解析逻辑从 Mapper 里抽出来，好处是它能被单元测试直接覆盖
 */
public class NcdcRecordParser {
  
  // NCDC 用 9999 表示「温度缺失」，这个值本身是无效数据
  private static final int MISSING_TEMPERATURE = 9999;
  
  // 解析出来的年份
  private String year;
  // 解析出来的气温
  private int airTemperature;
  // 解析出来的质量标记：只有 0、1、4、5、9 这几个值可信
  private String quality;
  
  /**
   * 解析一条记录，把结果填进实例字段
   *
   * <p>用固定下标截取是前提：NCDC 记录定长，字段位置不会变
   *
   * @param record 一整行记录
   */
  public void parse(String record) {
    // 年份占第 15~19 位
    year = record.substring(15, 19);
    // 先以字符串形式取出温度，因为可能要处理开头的正号
    String airTemperatureString;
    // Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
    // 气温可能带正号，而 parseInt 不接受正号（Java 7 之前），
    // 所以要把 '+' 去掉再解析
    if (record.charAt(87) == '+') { 
      // 去掉 '+' 后，温度实际占第 88~91 位
      airTemperatureString = record.substring(88, 92);
    // 不带正号，温度照旧占第 87~91 位
    } else {
      airTemperatureString = record.substring(87, 92);
    }
    // 解析成数值，之后才能做大小比较
    airTemperature = Integer.parseInt(airTemperatureString);
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
   * 判断这条记录的温度是否可信
   *
   * @return 温度不是缺失值、且质量标记可信时返回 true
   */
  public boolean isValidTemperature() {
    // 两个条件都要满足：不是 9999（缺失），质量位属于 {0,1,4,5,9}
    return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
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
// ^^ NcdcRecordParserV2
