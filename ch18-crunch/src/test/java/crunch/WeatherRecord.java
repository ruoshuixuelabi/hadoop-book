package crunch;

/**
 * 气象记录类
 *
 * 用于Avro序列化的气象数据POJO。
 * 包含年份、气温和气象站ID三个字段。
 *
 * 该类被Crunch的Avros.records()方法使用,
 * 需要有无参构造函数和getter方法。
 */
public class WeatherRecord {

    /** 年份 */
    private int year;

    /** 气温(华氏度) */
    private int temperature;

    /** 气象站ID */
    private String stationId;

    /**
     * 无参构造函数(Avro需要)
     */
    public WeatherRecord() {
    }

    /**
     * 全参数构造函数
     *
     * @param year 年份
     * @param temperature 气温
     * @param stationId 气象站ID
     */
    public WeatherRecord(int year, int temperature, String stationId) {
        this.year = year;
        this.temperature = temperature;
        this.stationId = stationId;
    }

    // ... getters elided

    /**
     * 获取年份
     *
     * @return 年份
     */
    public int getYear() {
        return year;
    }

    /**
     * 获取气温
     *
     * @return 气温(华氏度)
     */
    public int getTemperature() {
        return temperature;
    }

    /**
     * 获取气象站ID
     *
     * @return 气象站ID
     */
    public String getStationId() {
        return stationId;
    }

    /**
     * 判断相等
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WeatherRecord that = (WeatherRecord) o;

        if (temperature != that.temperature) return false;
        if (year != that.year) return false;
        if (stationId != null ? !stationId.equals(that.stationId) : that.stationId != null)
            return false;

        return true;
    }

    /**
     * 计算哈希码
     */
    @Override
    public int hashCode() {
        int result = year;
        result = 31 * result + temperature;
        result = 31 * result + (stationId != null ? stationId.hashCode() : 0);
        return result;
    }

    /**
     * 字符串表示
     */
    @Override
    public String toString() {
        return "WeatherRecord{" +
            "year=" + year +
            ", temperature=" + temperature +
            ", stationId='" + stationId + '\'' +
            '}';
    }
}