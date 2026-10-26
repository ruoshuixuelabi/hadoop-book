// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 汇聚同一键下的所有气温并输出其中的最大值
 *
 * <p>Reducer 收到的是「一个键 + 该键对应的全部值」，
 * 因此这里只需遍历一遍做归约，无需关心数据规模
 */
public class MaxTemperatureReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {
  
  /**
   * 遍历同一键下的所有气温值，取最大者输出
   *
   * @param key    分组键，本例为「年份+站号」
   * @param values 该键对应的全部气温值
   * @param context 作业上下文，用于把归约结果写出
   * @throws IOException 写出结果时发生 I/O 错误
   * @throws InterruptedException 作业被中断
   */
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    
    // 初值取整型最小值而非 0：气温可能为负，用 0 会导致负温度被错误抬高
    int maxValue = Integer.MIN_VALUE;
    for (IntWritable value : values) {
      maxValue = Math.max(maxValue, value.get());
    }
    context.write(key, new IntWritable(maxValue));
  }
}
// ^^ MaxTemperatureReducer
