// == StationPartitioner 自定义分区器，按气象站ID进行分区

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//vv StationPartitioner
/**
 * 自定义分区器示例，按气象站ID对记录进行分区。
 *
 * <p>默认情况下，MapReduce 使用 HashPartitioner 根据键的哈希值进行分区。
 * 本分区器演示如何根据记录内容（如气象站ID）进行自定义分区。
 *
 * <p>分区的作用：
 * <ul>
 *   <li>决定每条记录被发送到哪个 reduce 任务</li>
 *   <li>可以确保同一气象站的所有记录都被发送到同一个 reduce</li>
 *   <li>用于将相关数据放在一起进行处理</li>
 * </ul>
 *
 * <p>注意：本示例中的 getPartition(String stationId) 方法是 stub 实现，
 * 实际使用需要根据 stationId 返回有效的分区编号（0 到 numPartitions-1）。
 */
public class StationPartitioner extends Partitioner<LongWritable, Text> {
    // NCDC 记录解析器，用于从记录中提取气象站ID
    private NcdcRecordParser parser = new NcdcRecordParser();

    /**
     * 获取给定记录的分区编号。
     *
     * <p>该方法会被 MapReduce 框架调用，决定记录应该发送到哪个 reduce 任务。
     *
     * @param key          键（通常是行偏移量）
     * @param value        值（记录内容）
     * @param numPartitions 分区总数（即 reduce 任务数）
     * @return 分区编号，必须在 [0, numPartitions) 范围内
     */
    @Override
    public int getPartition(LongWritable key, Text value, int numPartitions) {
        // 解析记录以获取气象站ID
        parser.parse(value);
        // 根据气象站ID计算分区
        return getPartition(parser.getStationId());
    }

    /**
     * 根据气象站ID计算分区编号。
     *
     * <p>这是实际计算分区的逻辑。
     * 常见的实现方式包括：
     * <ul>
     *   <li>取模：stationId.hashCode() % numPartitions</li>
     *   <li>范围查找：根据气象站ID的前缀确定分区</li>
     *   <li>一致性哈希：用于负载均衡</li>
     * </ul>
     *
     * @param stationId 气象站ID
     * @return 分区编号
     */
    private int getPartition(String stationId) {
        /*...*/
        // ^^ StationPartitioner
        // 这里应该实现实际的分区逻辑
        return 0;
        // vv StationPartitioner
    }
}
//^^ StationPartitioner
