package oldapi;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * 按气象站号分区的分区器（v1 旧 API）
 *
 * <p>默认的 {@code HashPartitioner} 按键的哈希值分区，
 * 于是同一个气象站的记录可能被分散到不同的 Reducer 上
 * <p>本例改成按站号分区，保证同一站的数据一定落在同一个 Reducer，
 * 这样「取最高气温」这类操作就不必再跨节点合并
 */
public class StationPartitioner implements Partitioner<LongWritable, Text> {
  
  // 分区器要从整行记录里取出站号，因此自己解析
  private NcdcRecordParser parser = new NcdcRecordParser();
  
  /**
   * 计算一条记录应该分到哪个分区
   *
   * @param key 输入的键（本例用不到）
   * @param value 输入的值，即一行 NCDC 记录
   * @param numPartitions 分区总数
   * @return 分区编号
   */
  @Override
  public int getPartition(LongWritable key, Text value, int numPartitions) {
    // 先把记录解析开，站号就藏在里面
    parser.parse(value);
    // 解析出来的站号决定分区
    return getPartition(parser.getStationId());
  }

  /**
   * 由站号算出分区号
   *
   * <p>本例直接返回 0，即所有数据都进第一个分区；
   * 换成 {@code Math.abs(stationId.hashCode()) % numPartitions}
   * 之类的写法就能真正把数据打散
   *
   * @param stationId 站号
   * @return 分区编号
   */
  private int getPartition(String stationId) {
    // 永远返回第 0 个分区
    return 0;
  }

  /**
   * v1 的分区器配置钩子
   *
   * <p>本例不需要额外的配置，所以方法体是空的
   *
   * @param conf 作业配置
   */
  @Override
  public void configure(JobConf conf) { }
}
