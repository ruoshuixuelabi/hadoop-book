// cc NcdcRecordParserV2 A class for parsing weather records in NCDC format
package v2;

/**
 * NCDC（美国国家气候数据中心）气象记录解析器
 *
 * 该类用于解析NCDC格式的天气数据记录。NCDC格式是一种固定宽度的文本格式，
 * 每条记录包含年份、温度、空气质量等信息。此解析器支持温度数据的提取和验证。
 *
 * 主要功能：
 * - 从原始记录中提取年份字段（位置15-18）
 * - 从原始记录中提取温度字段（位置87-91），处理带加号的情况
 * - 从原始记录中提取质量码（位置92）
 * - 验证温度值是否有效（非9999且质量码合法）
 *
 * @see <a href="http://www.ncdc.noaa.gov/">NCDC</a>
 */
import org.apache.hadoop.io.Text;

// vv NcdcRecordParserV2
public class NcdcRecordParser {

    /**
     * 缺失温度标记值
     * NCDC数据中，9999表示温度数据缺失
     */
    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemperature;
    private String quality;

    /**
     * 解析字符串格式的NCDC记录
     *
     * @param record NCDC格式的原始记录字符串
     */
    public void parse(String record) {
        // 从位置15-18提取4位年份
        year = record.substring(15, 19);

        String airTemperatureString;
        // 移除前导加号，因为Integer.parseInt不识别它（Java 7之前的版本）
        // 温度字段位置87，如果第87个字符是'+'，则温度从位置88开始
        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
        }
        else {
            airTemperatureString = record.substring(87, 92);
        }
        airTemperature = Integer.parseInt(airTemperatureString);
        // 从位置92-93提取1位质量码
        quality = record.substring(92, 93);
    }

    /**
     * 解析Text类型的NCDC记录
     * 将Text对象转换为字符串后调用parse方法
     *
     * @param record NCDC格式的Text记录
     */
    public void parse(Text record) {
        parse(record.toString());
    }

    /**
     * 判断温度值是否有效
     *
     * 有效的温度必须满足：
     * 1. 温度值不等于缺失标记值9999
     * 2. 质量码匹配合法值集合[01459]
     *
     * @return 如果温度有效返回true，否则返回false
     */
    public boolean isValidTemperature() {
        return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }

    /**
     * 获取解析后的年份
     *
     * @return 4位年份字符串
     */
    public String getYear() {
        return year;
    }

    /**
     * 获取解析后的空气温度
     *
     * @return 温度整数值（华氏度）
     */
    public int getAirTemperature() {
        return airTemperature;
    }
}
// ^^ NcdcRecordParserV2