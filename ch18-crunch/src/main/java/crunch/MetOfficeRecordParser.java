package crunch;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;

/**
 * Met Office气象数据解析器
 * <p>
 * 用于解析英国气象局(Met Office)格式的气象数据记录。
 * 该解析器实现了Serializable接口,可以在Crunch管道中序列化传输。
 * <p>
 * Met Office数据格式(固定宽度文本):
 * - 位置3-7: 年份
 * - 位置13-18: 气温(可能为空或"---")
 * <p>
 * 示例记录:
 * 0195010071900004-001-009... (1950年,气温-0.1度)
 * <p>
 * 气温转换:
 * - 原始数据: -0.1(华氏度,一位小数)
 * - 存储值: -1(放大10倍,去掉小数点)
 * - 转换公式: temp * 10
 * <p>
 * 与NCDC格式的区别:
 * - 不同的列位置
 * - 不同的缺失值表示(--- vs 9999)
 * - 不同的气象站ID格式
 */
public class MetOfficeRecordParser implements Serializable {

    /**
     * 年份字符串
     */
    private String year;

    /**
     * 气温字符串表示
     */
    private String airTemperatureString;

    /**
     * 气温整数值(放大10倍)
     */
    private int airTemperature;

    /**
     * 气温是否有效
     */
    private boolean airTemperatureValid;

    /**
     * 解析Met Office记录
     *
     * @param record 气象数据行
     */
    public void parse(String record) {
        // 检查最小长度(表头行较短)
        if (record.length() < 18) {
            return;
        }

        // 解析年份(位置3-7)
        year = record.substring(3, 7);

        // 验证年份并解析气温
        if (isValidRecord(year)) {
            // 提取气温字符串(位置13-18)
            airTemperatureString = record.substring(13, 18);

            // 检查是否缺失(---表示缺失)
            if (!airTemperatureString.trim().equals("---")) {
                // 转换: 气温 * 10
                // 例如: -0.1 -> -1, 78.5 -> 785
                BigDecimal temp = new BigDecimal(airTemperatureString.trim());
                temp = temp.multiply(new BigDecimal(BigInteger.TEN));
                airTemperature = temp.intValueExact();
                airTemperatureValid = true;
            }
        }
    }

    /**
     * 验证记录是否有效
     *
     * @param year 年份字符串
     * @return true如果年份是有效的数字
     */
    private boolean isValidRecord(String year) {
        try {
            Integer.parseInt(year);
            return true;
        }
        catch (NumberFormatException e) {
            return false;
        }
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
     * 获取年份字符串
     *
     * @return 年份字符串
     */
    public String getYear() {
        return year;
    }

    /**
     * 获取气温整数值
     * <p>
     * 注意:
     */
    public int getAirTemperature() {
        return airTemperature;
    }

    public String getAirTemperatureString() {
        return airTemperatureString;
    }

    public boolean isValidTemperature() {
        return airTemperatureValid;
    }

}