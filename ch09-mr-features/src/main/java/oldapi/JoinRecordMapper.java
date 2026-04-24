package oldapi;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * 气象记录标记Mapper,用于Reduce端连接(旧API版本).
 *
 * 本程序是JoinRecordMapper的旧API版本,
 * 使用org.apache.hadoop.mapred包中的旧接口.
 *
 * 功能说明:
 * - 解析NCDC气象数据记录
 * - 提取站点ID和标记"1"输出
 * - 确保在同站点站点记录("0")之后处理
 *
 * @see JoinRecordWithStationName
 */
public class JoinRecordMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, TextPair, Text> {
    private NcdcRecordParser parser = new NcdcRecordParser();

    /**
     * Map方法:解析气象记录并标记.
     *
     * @param key        行偏移量
     * @param value      NCDC数据行
     * @param output     输出收集器
     * @param reporter   作业Reporter
     * @throws IOException IO异常
     */
    public void map(LongWritable key, Text value,
        OutputCollector<TextPair, Text> output, Reporter reporter)
        throws IOException {

        parser.parse(value);
        // 输出:(站点ID, "1"), 标记"1"表示气象记录
        output.collect(new TextPair(parser.getStationId(), "1"), value);
    }
}