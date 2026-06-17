package v4;

/**
 * NCDC气象记录解析器（版本4）
 * <p>
 * 这是解析器的增强版本，相比v2版本增加了对格式错误温度的检测能力。
 * <p>
 * NCDC记录格式说明：<br/>
 * - 位置15-18：年份（4位数字）<br/>
 * - 位置87-91：温度（可能以'+'或'-'开头）<br/>
 * - 位置92：质量码
 * <p>
 * 温度数据说明：<br/>
 * - 9999表示温度缺失<br/>
 * - 有效的质量码包括：0、1、4、5、9
 * <p>
 * 增强功能：<br/>
 * - 能够检测格式错误的温度数据（如非数字字符导致解析失败）
 *
 * @version 4.0
 */
import org.apache.hadoop.io.Text;

public class NcdcRecordParser {

    /**
     * 缺失温度标记值
     * <p/>
     * NCDC数据中使用9999表示温度数据缺失
     */
    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemperature;
    /**
     * 标记温度数据是否格式错误
     * <p/>
     * 当无法正确解析温度时设为true
     */
    private boolean airTemperatureMalformed;
    private String quality;

    /**
     * 解析NCDC格式的字符串记录
     * <p/>
     * 提取年份（位置15-18）<br/>
     * 移除前导加号，处理正负温度<br/>
     * 提取质量码（位置92）
     *
     * @param record NCDC格式的原始记录
     */
    public void parse(String record) {
        // 提取年份（位置15-18）
        year = record.substring(15, 19);
        airTemperatureMalformed = false;
        // 移除前导加号，因为Integer.parseInt不识别它（Java 7之前的版本）
        // 检查温度字段的格式：支持'+'（正温度）、'-'（负温度）
        if (record.charAt(87) == '+') {
            // 正温度，处理加号后的数字
            airTemperature = Integer.parseInt(record.substring(88, 92));
        } else if (record.charAt(87) == '-') {
            // 负温度，包含负号
            airTemperature = Integer.parseInt(record.substring(87, 92));
        } else {
            // 格式错误：既不是'+'也不是'-'，可能是数据损坏
            airTemperatureMalformed = true;
        }
        // 提取质量码（位置92）
        quality = record.substring(92, 93);
    }

    /**
     * 解析Text类型的记录
     * <p/>
     * 将Text对象转换为字符串后调用parse方法
     *
     * @param record NCDC格式的Text记录
     */
    public void parse(Text record) {
        parse(record.toString());
    }

    /**
     * 判断温度是否有效
     * <p>
     * 有效温度必须同时满足：<br/>
     * 1. 格式正确（非airTemperatureMalformed）<br/>
     * 2. 不是缺失值（不等于9999）<br/>
     * 3. 质量码合法（匹配[01459]）
     *
     * @return 如果温度有效返回true
     */
    public boolean isValidTemperature() {
        return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
            && quality.matches("[01459]");
    }

    /**
     * 判断温度是否格式错误
     * <p>
     * 当温度字段包含无法解析的字符时返回true
     *
     * @return 如果温度格式错误返回true
     */
    public boolean isMalformedTemperature() {
        return airTemperatureMalformed;
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