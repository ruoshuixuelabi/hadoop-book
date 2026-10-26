// cc JoinRecordMapper Mapper for tagging weather records for a reduce-side join
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//vv JoinRecordMapper
/**
 * 给天气记录打标记的 Mapper，用于 Reducer 端的连接
 *
 * <p>连接（join）分两种：Map 端连接要把两条大表都放进内存，代价很高；
 * Reducer 端连接则把两条表都按连接键分组，再由 Reducer 把同组的记录配对
 * <p>本例属于后者：把天气记录按「站号 + 标记 1」输出，
 * 与站点信息（标记 0）汇合到同一个分组里
 */
public class JoinRecordMapper
    extends Mapper<LongWritable, Text, TextPair, Text> {
  // 解析器实例：一个 Map 任务只创建一次
  private NcdcRecordParser parser = new NcdcRecordParser();
  
  /**
   * 解析记录并按「站号 + 标记」输出
   *
   * @param key 输入的键（本例用不到）
   * @param value 输入的值，即一行 NCDC 记录
   * @param context 上下文，用来写出结果
   * @throws IOException 写出失败
   * @throws InterruptedException 任务被中断
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 解析这一行记录，取出站号等字段
    parser.parse(value);
    // 键的第二段是标记 1，表示这条是天气记录
    context.write(new TextPair(parser.getStationId(), "1"), value);
  }

}
//^^ JoinRecordMapper