package oldapi;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * 自定义分区器示例，按气象站ID进行分区（旧 API）。
 *
 * <p>本分区器演示如何根据记录内容自定义分区逻辑。
 * 实际使用需要完成 getPartition(String stationId) 方法的实现。
 */
public class StationPartitioner implements Partitioner<LongWritable, Text> {

    private NcdcRecordParser parser = new NcdcRecordParser();

    /**
     * 获取给定记录的分区编号。
     *
     * @param key          键（行偏移量）
     * @param value        值（记录内容）
     * @param numPartitions 分区总数
     * @return 分区编号
     */
    @Override
    public int getPartition(LongWritable key, Text value, int numPartitions) {
        parser.parse(value);
        return getPartition(parser.getStationId());
    }

    /**
     * 根据气象站ID计算分区编号。
     *
     * <p>注意：这是 stub 实现，实际使用需要根据 stationId 返回有效的分区编号。
     *
     * @param stationId 气象站ID
     * @return 分区编号
     */
    private int getPartition(String stationId) {
        return 0;
    }

    /**
     * 配置分区器。
     * 在分区器初始化时调用。
     *
     * @param conf JobConf 配置
     */
    @Override
    public void configure(JobConf conf) { }
}
