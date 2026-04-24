import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.io.Text;

/**
 * NCDC (美国国家气候数据中心) 气象记录解析器。
 *
 * <p>此类负责解析 NCDC 格式的天气数据文件，每条记录为固定长度的文本行。
 * NCDC 数据格式是气象领域广泛使用的标准格式。
 *
 * <p>解析的字段包括：
 * <ul>
 *   <li>stationId: 气象站 ID (由记录中的特定位置提取并格式化)</li>
 *   <li>observationDate: 观测日期时间</li>
 *   <li>year: 年份</li>
 *   <li>airTemperature: 空气温度</li>
 *   <li>quality: 数据质量标识</li>
 * </ul>
 *
 * <p>温度值的处理：
 * <ul>
 *   <li>9999 表示温度缺失</li>
 *   <li>以 '+' 开头表示正温度 (Java 7 之前 parseInt 不支持 '+' 前缀)</li>
 *   <li>以 '-' 开头表示负温度</li>
 *   <li>质量标识必须是 [01459] 才认为是有效数据</li>
 * </ul>
 *
 * @see <a href="http://www.ncdc.noaa.gov/data-access/land-based-station-data">NCDC Data Access</a>
 */
public class NcdcRecordParser {

    /** 温度缺失的标记值 */
    private static final int MISSING_TEMPERATURE = 9999;

    /** NCDC 日期时间格式: yyyyMMddHHmm (例如: 201501011200) */
    private static final DateFormat DATE_FORMAT =
    new SimpleDateFormat("yyyyMMddHHmm");

    private String stationId;
    private String observationDateString;
    private String year;
    private String airTemperatureString;
    private int airTemperature;
    private boolean airTemperatureMalformed;
    private String quality;

    /**
     * 解析 NCDC 格式的字符串记录。
     *
     * <p>记录格式 (固定宽度):
     * <ul>
     *   <li>位置 4-10: 气象站 ID 前 6 位</li>
     *   <li>位置 10-15: 气象站 ID 后 5 位</li>
     *   <li>位置 15-19: 年份</li>
     *   <li>位置 19-27: 观测日期时间 (yyyyMMddHHmm)</li>
     *   <li>位置 87: 温度符号位 ('+' 或 '-')</li>
     *   <li>位置 88-92: 温度值 (如果以 '+' 开头)</li>
     *   <li>位置 87-92: 温度值 (如果以 '-' 开头)</li>
     *   <li>位置 92-93: 质量标识</li>
     * </ul>
     *
     * @param record NCDC 格式的原始记录行
     */
    public void parse(String record) {
        // 提取气象站 ID，格式为 "XXXXXX-XXXXX"
        stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
        // 观测日期时间字符串
        observationDateString = record.substring(15, 27);
        // 年份
        year = record.substring(15, 19);
        airTemperatureMalformed = false;
        // 移除前导 '+' 符号，因为 parseInt 不支持 (Java 7 之前)
        // NCDC 数据中温度字段以 '+' 表示正温度
        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
            airTemperature = Integer.parseInt(airTemperatureString);
        } else if (record.charAt(87) == '-') {
            airTemperatureString = record.substring(87, 92);
            airTemperature = Integer.parseInt(airTemperatureString);
        } else {
            // 不是 '+' 或 '-'，说明温度格式错误
            airTemperatureMalformed = true;
        }
        airTemperature = Integer.parseInt(airTemperatureString);
        // 质量标识 (0, 1, 4, 5, 9 为有效值)
        quality = record.substring(92, 93);
    }

    /**
     * 重载方法，支持 Text 类型输入。
     *
     * @param record Hadoop Text 类型的记录
     */
    public void parse(Text record) {
        parse(record.toString());
    }

    /**
     * 判断温度是否有效。
     *
     * <p>有效温度需满足：
     * <ul>
     *   <li>温度格式正确 (非 malformed)</li>
     *   <li>温度不是缺失值 (9999)</li>
     *   <li>质量标识在有效范围内 [01459]</li>
     * </ul>
     *
     * @return true 如果温度有效
     */
    public boolean isValidTemperature() {
        return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
        && quality.matches("[01459]");
    }

    public boolean isMalformedTemperature() {
        return airTemperatureMalformed;
    }

    public boolean isMissingTemperature() {
        return airTemperature == MISSING_TEMPERATURE;
    }

    public String getStationId() {
        return stationId;
    }

    /**
     * 解析并返回观测日期。
     *
     * @return Date 对象
     * @throws IllegalArgumentException 如果日期格式错误
     */
    public Date getObservationDate() {
        try {
            System.out.println(observationDateString);
            return DATE_FORMAT.parse(observationDateString);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String getYear() {
        return year;
    }

    /**
     * 返回年份的整数值。
     *
     * @return 年份 (如 2015)
     */
    public int getYearInt() {
        return Integer.parseInt(year);
    }

    public int getAirTemperature() {
        return airTemperature;
    }

    public String getAirTemperatureString() {
        return airTemperatureString;
    }

    public String getQuality() {
        return quality;
    }

}