import org.apache.hadoop.hbase.util.Bytes;

/**
 * 生成 HBase 观测表的行键
 *
 * <p>行键的布局是「站号 + 反序时间戳」：
 * 站号固定占 12 个字节，保证同一个站的记录在存储上挨在一起；
 * 时间戳取反后再放进去，这样按行键顺序扫描就是按时间倒序
 */
public class RowKeyConverter {

  // 站号在行键里占的字节数
  // 定长才能保证「站号 + 时间戳」这个布局稳定
  private static final int STATION_ID_LENGTH = 12;

  /**
   * 用站号和观测时间拼出行键
   *
   * <p>行键的格式是：{@code <station_id> <reverse_order_timestamp>}
   * <p>时间戳取反是为了让「最新的观测排在最前面」：
   * HBase 按行键的字节序排序，
   * 而时间越大取反后越小，于是就变成了倒序
   *
   * @return A row key whose format is: <station_id> <reverse_order_timestamp>
   */
  public static byte[] makeObservationRowKey(String stationId,
      long observationTime) {
    // 行键 = 站号占的 12 字节 + 时间戳占的 8 字节
    byte[] row = new byte[STATION_ID_LENGTH + Bytes.SIZEOF_LONG];
    // 把站号写进行键的前 12 个字节
    // 只取前 12 个字节，多出来的部分会被截掉
    Bytes.putBytes(row, 0, Bytes.toBytes(stationId), 0, STATION_ID_LENGTH);
    // 时间戳取反：用最大值减去原值
    long reverseOrderTimestamp = Long.MAX_VALUE - observationTime;
    // 把取反后的时间戳写进行键的后 8 个字节
    Bytes.putLong(row, STATION_ID_LENGTH, reverseOrderTimestamp);
    // 返回拼好的行键
    return row;
  }
}
