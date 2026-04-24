package crunch;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.Text;

/**
 * NCDC气象数据解析器
 *
 * 用于解析NCDC(美国国家气候数据中心)格式的气象数据记录。
 * 该解析器实现了Serializable接口,可以在Crunch管道中序列化传输。
 *
 * NCDC数据格式(固定宽度文本):
 * - 位置4-10: 气象站USAFA标识
 * - 位置10-15: WBAN标识
 * - 位置15-19: 年份
 * - 位置15-27: 观测日期时间(yyyyMMddHHmm)
 * - 位置87-92: 气温(可能带符号)
 * - 位置92-93: 质量标识
 *
 * 示例记录:
 * 0043011950999991950010112004+68750+086550FM-12+000299999V0200401N002819999999N0000001N9-00781+99999999999
 *
 * 特殊值说明:
 * - 9999: 缺失气温值
 * - +: 正温度前导符号(parseInt不支持)
 * - -: 负温度前导符号
 *
 * 质量标识有效值: 01459
 */
public class NcdcRecordParser implements Serializable {

    /** 缺失气温标记值 */
    private static final int MISSING_TEMPERATURE = 9999;

    /** 日期格式: yyyyMMddHHmm */
    private static final DateFormat DATE_FORMAT =
        new SimpleDateFormat("yyyyMMddHHmm");

    /** 气象站ID (USAF-WBAN格式) */
    private String stationId;

    /** 观测日期字符串 */
    private String observationDateString;

    /** 年份字符串 */
    private String year;

    /** 气温字符串表示 */
    private String airTemperatureString;

    /** 气温整数值 */
    private int airTemperature;

    /** 气温格式是否错误 */
    private boolean airTemperatureMalformed;

    /** 质量标识 */
    private String quality;

    /**
     * 解析NCDC记录字符串
     *
     * @param record NCDC数据行
     */
    public void parse(String record) {
        // 解析气象站ID (USAF-WBAN格式)
        stationId = record.substring(4, 10) + "-" + record.substring(10, 15);

        // 解析观测日期
        observationDateString = record.substring(15, 27);

        // 解析年份
        year = record.substring(15, 19);

        // 重置格式错误标志
        airTemperatureMalformed = false;

        // 解析气温,处理前导符号
        // Java 7之前的parseInt不支持前导+号
        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
            airTemperature = Integer.parseInt(airTemperatureString);
        } else if (record.charAt(87) == '-') {
            airTemperatureString = record.substring(87, 92);
            airTemperature = Integer.parseInt(airTemperatureString);
        } else {
            // 既不是+也不是-,说明格式错误
            airTemperatureMalformed = true;
        }
        // 再次解析(对于非+/-开头的情况)
        airTemperature = Integer.parseInt(airTemperatureString);

        // 解析质量标识
        quality = record.substring(92, 93);
    }

    /**
     * 解析Hadoop Text类型记录
     *
     * @param record Hadoop Text类型记录
     */
    public void parse(Text record) {
        parse(record.toString());
    }

    /**
     * 检查气温是否有效
     *
     * 有效条件:
     * - 格式正确(非Malformed)
     * - 不是缺失值(9999)
     * - 质量标识有效(匹配[01459])
     *
     * @return true如果气温有效
     */
    public boolean isValidTemperature() {
        return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
            && quality.matches("[01459]");
    }

    /**
     * 检查气温格式是否错误
     *
     * @return true如果格式错误
     */
    public boolean isMalformedTemperature() {
        return airTemperatureMalformed;
    }

    /**
     * 检查气温是否缺失
     *
     * @return true如果气温是缺失值
     */
    public boolean isMissingTemperature() {
        return airTemperature == MISSING_TEMPERATURE;
    }

    /**
     * 获取气象站ID
     *
     * @return 气象站ID (USAF-WBAN格式)
     */
    public String getStationId() {
        return stationId;
    }

    /**
     * 获取观测日期
     *
     * @return Date对象
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

    /**
     * 获取年份字符串
     *
     * @return 年份字符串(如"1950")
     */
    public String getYear() {
        return year;
    }

    /**
     * 获取年份整数
     *
     * @return 年份整数(如1950)
     */
    public int getYearInt() {
        return Integer.parseInt(year);
    }

    /**
     * 获取气温整数值
     *
     * @return 气温值(华氏度)
     */
    public int getAirTemperature() {
        return airTemperature;
    }

    /**
     * 获取气温字符串
     *
     * @return 气温字符串
     */
    public String getAirTemperatureString() {
        return airTemperatureString;
    }

    /**
     * 获取质量标识
     *
     * @return 质量标识字符
     */
    public String getQuality() {
        return quality;
    }

}