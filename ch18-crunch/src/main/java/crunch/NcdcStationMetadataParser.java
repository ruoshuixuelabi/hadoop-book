package crunch;

import java.io.Serializable;
import org.apache.hadoop.io.Text;

/**
 * NCDC气象站元数据解析器
 *
 * 用于解析NCDC气象站元数据文件,提取气象站ID和名称。
 * 该解析器实现了Serializable接口,可以在Crunch管道中序列化传输。
 *
 * 元数据格式(固定宽度文本):
 * - 位置0-6: USAF气象站标识
 * - 位置7: 分隔符(-)
 * - 位置7-12: WBAN气象站标识
 * - 位置13-42: 气象站名称
 *
 * 示例记录:
 * 012650-99999  USC00150015   NEW YORK CITY CENTRAL PARK
 *
 * 气象站ID格式: USAF-WBAN (如012650-99999)
 *
 * 注意:
 * - USAF标识是数字,如果解析失败说明是表头或无效行
 * - 气象站名称可能包含空格
 */
public class NcdcStationMetadataParser implements Serializable {

    /** 气象站ID (USAF-WBAN格式) */
    private String stationId;

    /** 气象站名称 */
    private String stationName;

    /**
     * 解析气象站元数据记录
     *
     * @param record 元数据行
     * @return true如果解析成功(有效的USAF标识)
     */
    public boolean parse(String record) {
        // 检查最小长度(表头行较短)
        if (record.length() < 42) { // header
            return false;
        }

        // 提取USAF标识(位置0-6)
        String usaf = record.substring(0, 6);

        // 提取WBAN标识(位置7-12)
        String wban = record.substring(7, 12);

        // 组合气象站ID
        stationId = usaf + "-" + wban;

        // 提取气象站名称(位置13-42)
        stationName = record.substring(13, 42);

        try {
            // USAF标识必须是数字,用于验证记录有效性
            Integer.parseInt(usaf); // USAF identifiers are numeric
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * 解析Hadoop Text类型记录
     *
     * @param record Hadoop Text类型记录
     * @return true如果解析成功
     */
    public boolean parse(Text record) {
        return parse(record.toString());
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
     * 获取气象站名称
     *
     * @return 气象站名称
     */
    public String getStationName() {
        return stationName;
    }

}