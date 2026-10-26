package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * 把站点名与天气记录拼在一起的 Reducer（v1 版）
 *
 * <p>同一分组的记录里，第一条是站点信息（标记 0），
 * 后面跟着该站点的所有天气记录（标记 1）
 * <p>Reducer 把站点名与每条天气记录拼成一行输出，
 * 键则是复合键的第一段（站号）
 */
public class JoinReducer extends MapReduceBase implements
    Reducer<TextPair, Text, Text, Text> {

  /**
   * 把站点名与本组的每条天气记录配对输出
   *
   * @param key 复合键，第一段是站号
   * @param values 本组的所有记录：先是站点名，后是天气记录
   * @param output 结果收集器
   * @param reporter 状态报告器
   * @throws IOException 写出失败
   */
  public void reduce(TextPair key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {

    // 第一条一定是站点名：它的标记是 0，排序时排在前面
    Text stationName = new Text(values.next());
    // 剩下的每一条都是天气记录
    while (values.hasNext()) {
      // 取出一条天气记录
      Text record = values.next();
      // 把站点名与记录用制表符拼成一行
      Text outValue = new Text(stationName.toString() + "\t" + record.toString());
      // 输出的键是站号（复合键的第一段）
      output.collect(key.getFirst(), outValue);
    }
  }
}
