// cc JoinRecordMapper Mapper:为Reduce端连接标记气象记录

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//vv JoinRecordMapper

/**
 * 气象记录标记Mapper,用于Reduce端连接.
 *
 * 功能说明:
 * - 解析NCDC气象数据记录
 * - 提取站点ID作为TextPair的第一字段
 * - 使用标记"1"标识气象记录,确保在同站点站点记录之后处理
 *
 * 连接原理:
 * Join操作需要站点名称在气象记录之前到达Reducer,
 * 站点记录标记为"0",气象记录标记为"1",
 * 在分组时同站点数据会聚合,遍历时第一条就是站点名称.
 *
 * 数据流:
 * 输入:(偏移量, NCDC记录)
 * 输出键:TextPair(站点ID, "1")
 * 输出值:NCDC记录原文
 *
 * @see JoinRecordWithStationName
 * @see JoinStationMapper
 * @see TextPair
 */
public class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    private NcdcRecordParser parser = new NcdcRecordParser();

    /**
     * Map方法:解析气象记录并标记.
     *
     * @param key     行偏移量
     * @param value   NCDC数据行
     * @param context MapReduce上下文
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        // 输出:(站点ID, "1"), 标记"1"表示气象记录
        context.write(new TextPair(parser.getStationId(), "1"), value);
    }
}
//^^ JoinRecordMapper