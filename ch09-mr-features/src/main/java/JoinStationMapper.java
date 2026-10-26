// cc JoinStationMapper Mapper for tagging station records for a reduce-side join
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

// vv JoinStationMapper
/**
 * 给站点记录打标记的 Mapper，用于 Reducer 端的连接
 *
 * <p>站点元数据文件里每行包含站号、站名等信息，
 * 本例把站号与站名取出来，按「站号 + 标记 0」输出
 * <p>标记 0 与天气记录的标记 1 一起，
 * 使同一站点的两类记录在 Reducer 端汇合到同一个分组里
 */
public class JoinStationMapper
    extends Mapper<LongWritable, Text, TextPair, Text> {
  // 站点元数据解析器实例
  private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

  /**
   * 解析站点记录并按「站号 + 标记」输出
   *
   * @param key 输入的键（本例用不到）
   * @param value 输入的值，即一行站点记录
   * @param context 上下文，用来写出结果
   * @throws IOException 写出失败
   * @throws InterruptedException 任务被中断
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 只有能解析出站号与站名的记录才参与连接
    if (parser.parse(value)) {
      // 键的第二段是标记 0，表示这条是站点信息
      context.write(new TextPair(parser.getStationId(), "0"),
          new Text(parser.getStationName()));
    }
  }
}
// ^^ JoinStationMapper