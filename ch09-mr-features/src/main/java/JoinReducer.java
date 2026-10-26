// cc JoinReducer Reducer for joining tagged station records with tagged weather records
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// vv JoinReducer
/**
 * 把站点名与天气记录拼在一起的 Reducer
 *
 * <p>同一分组的记录里，第一条是站点信息（标记 0），
 * 后面跟着该站点的所有天气记录（标记 1）
 * <p>Reducer 把站点名与每条天气记录拼成一行输出，
 * 键则是复合键的第一段（站号）
 */
public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

  /**
   * 把站点名与本组的每条天气记录配对输出
   *
   * @param key 复合键，第一段是站号
   * @param values 本组的所有记录：先是站点名，后是天气记录
   * @param context 上下文，用来写出结果
   * @throws IOException 写出失败
   * @throws InterruptedException 任务被中断
   */
  @Override
  protected void reduce(TextPair key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    // 本组的记录：先是站点名，后面跟着该站的天气记录
    Iterator<Text> iter = values.iterator();
    // 第一条一定是站点名：它的标记是 0，排序时排在前面
    Text stationName = new Text(iter.next());
    // 剩下的每一条都是天气记录
    while (iter.hasNext()) {
      // 取出一条天气记录
      Text record = iter.next();
      // 把站点名与记录用制表符拼成一行
      Text outValue = new Text(stationName.toString() + "\t" + record.toString());
      // 输出的键是站号（复合键的第一段）
      context.write(key.getFirst(), outValue);
    }
  }
}
// ^^ JoinReducer