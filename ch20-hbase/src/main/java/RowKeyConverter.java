import org.apache.hadoop.hbase.util.Bytes;

/**
 * 行键(RowKey)转换工具类
 *
 * 提供将气象观测数据组合成 HBase 行键的功能。
 * 行键设计对于 HBase 的查询性能和数据组织至关重要。
 *
 * 行键格式:
 * - 长度: 20 字节 (12 字节站点ID + 8 字节时间戳)
 * - 结构: <station_id_12bytes><reverse_timestamp_8bytes>
 *
 * 逆序时间戳设计:
 * - 存储的实际值 = Long.MAX_VALUE - 真实时间戳
 * - 效果: 最新的数据行键值最大,排在前面
 * - 优点: Scan 时自然按时间倒序返回,无需额外过滤
 *
 * /*[*
 * 行键设计考量:
 * 1. 固定长度便于解析和比较
 * 2. 站点ID放在前面支持按站点查询
 * 3. 逆序时间戳使新数据排在前面
 * *]*/
public class RowKeyConverter {

  // 站点ID字段的长度(字节),气象站点ID固定为12个字符
  private static final int STATION_ID_LENGTH = 12;

  /**
   * 生成观测数据的 HBase 行键
   *
   * 将站点ID和观测时间组合成一个字节数组作为行键。
   * 行键格式: station_id(12字节,不足补空白) + reverse_timestamp(8字节)
   *
   * 逆序时间戳实现:
   * - 真实时间戳 observationTime 是从 1970-01-01 开始的毫秒数
   * - 存储时用 Long.MAX_VALUE - observationTime
   * - 查询时: observationTime = Long.MAX_VALUE - 存储值
   * - 结果: 较新的数据行键值更大,Scan 时排在前面
   *
   * @param stationId 气象站点ID (固定12字符)
   * @param observationTime 观测时间戳(毫秒)
   * @return 20字节的行键字节数组
   */
  public static byte[] makeObservationRowKey(String stationId,
      long observationTime) {
    // 创建 20 字节的数组 (12 station_id + 8 timestamp)
    byte[] row = new byte[STATION_ID_LENGTH + Bytes.SIZEOF_LONG];

    // 将站点ID复制到行键的前 12 字节
    // Bytes.toBytes(stationId) 将 String 转为字节数组
    // 目标位置: row[0] 开始,复制 12 字节
    Bytes.putBytes(row, 0, Bytes.toBytes(stationId), 0, STATION_ID_LENGTH);

    // 计算逆序时间戳: Long.MAX_VALUE - observationTime
    // 这样较新的时间(较大 observationTime)会得到较小的存储值
    long reverseOrderTimestamp = Long.MAX_VALUE - observationTime;

    // 将逆序时间戳写入行键的后 8 字节
    // 目标位置: row[12] 开始 (STATION_ID_LENGTH 偏移)
    Bytes.putLong(row, STATION_ID_LENGTH, reverseOrderTimestamp);

    return row;
  }
}