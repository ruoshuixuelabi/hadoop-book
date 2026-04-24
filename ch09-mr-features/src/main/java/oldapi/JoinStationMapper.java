package oldapi;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * 站点元数据标记Mapper,用于Reduce端连接(旧API版本).
 *
 * 本程序是JoinStationMapper的旧API版本,
 * 使用org.apache.hadoop.mapred包中的旧接口.
 *
 * 功能说明:
 * - 解析站点元数据文件
 * - 提取站点ID和站点名称
 * - 使用标记"0"确保在同站点气象记录之前处理
 *
 * @see JoinRecordWithStationName
 */
public class JoinStationMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, TextPair, Text> {
    private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

    /**
     * Map方法:解析站点元数据并标记.
     *
     * @param key        行偏移量
     * @param value      站点元数据行
     * @param output     输出收集器
     * @param reporter   作业Reporter
     * @throws IOException IO异常
     */
    public void map(LongWritable key, Text value,
        OutputCollector<TextPair, Text> output, Reporter reporter)
        throws IOException {

        if (parser.parse(value)) {
            // 输出:(站点ID, "0"), 标记"0"表示站点记录
            output.collect(new TextPair(parser.getStationId(), "0"),
                new Text(parser.getStationName()));
        }
    }
}