// cc JoinStationMapper Mapper:为Reduce端连接标记站点记录

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

// vv JoinStationMapper

/**
 * 站点元数据标记Mapper,用于Reduce端连接.
 *
 * 功能说明:
 * - 解析站点元数据文件(包含站点ID和站点名称)
 * - 提取站点ID和站点名称
 * - 使用标记"0"标识站点记录,确保在同站点气象记录之前处理
 *
 * 连接原理:
 * - 站点记录标记为"0",气象记录标记为"1"(见JoinRecordMapper)
 * - 由于TextPair.FirstComparator按站点ID分组,
 *   组内数据按第二字段排序,故"0"会在"1"之前
 * - Reducer处理时,values迭代器的第一个就是站点名称
 *
 * 数据流:
 * 输入:(偏移量, 站点元数据行)
 * 输出键:TextPair(站点ID, "0")
 * 输出值:站点名称
 *
 * @see JoinRecordWithStationName
 * @see JoinRecordMapper
 * @see JoinReducer
 * @see TextPair
 */
public class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

    /**
     * Map方法:解析站点元数据并标记.
     *
     * @param key     行偏移量
     * @param value   站点元数据行
     * @param context MapReduce上下文
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (parser.parse(value)) {
            // 输出:(站点ID, "0"), 标记"0"表示站点记录
            context.write(new TextPair(parser.getStationId(), "0"), new Text(parser.getStationName()));
        }
    }
}
// ^^ JoinStationMapper