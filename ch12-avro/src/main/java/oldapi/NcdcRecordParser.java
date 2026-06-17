package oldapi;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.Text;

/**
 * NCDC (美国国家气候数据中心) 气象记录解析器 - 旧 API 版本。
 *
 * <p>与主版本 {@link NcdcRecordParser} 功能相同，
 * 此类位于 oldapi 包中，供旧 MapReduce API 使用。
 *
 * <p>负责解析 NCDC 格式的天气数据文件，
 * 提取气象站 ID、日期、温度等字段。
 *
 * @see NcdcRecordParser 主版本解析器
 */
public class NcdcRecordParser {

    /** 温度缺失的标记值 */
    private static final int MISSING_TEMPERATURE = 9999;

    /** NCDC 日期时间格式: yyyyMMddHHmm */
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
     * @param record NCDC 格式的原始记录行
     */
    public void parse(String record) {
        // 提取气象站 ID，格式为 "XXXXXX-XXXXX"
        stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
        observationDateString = record.substring(15, 27);
        year = record.substring(15, 19);
        airTemperatureMalformed = false;
        // 移除前导 '+' 符号，因为 parseInt 不支持 (Java 7 之前)
        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
            airTemperature = Integer.parseInt(airTemperatureString);
        } else if (record.charAt(87) == '-') {
            airTemperatureString = record.substring(87, 92);
            airTemperature = Integer.parseInt(airTemperatureString);
        } else {
            airTemperatureMalformed = true;
        }
        airTemperature = Integer.parseInt(airTemperatureString);
        quality = record.substring(92, 93);
    }

    /**
     * 重载方法，支持 Hadoop Text 类型输入。
     *
     * @param record Hadoop Text 类型的记录
     */
    public void parse(Text record) {
        parse(record.toString());
    }

    /**
     * 判断温度是否有效。
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