package v1;
//cc MaxTemperatureReducerV1 Reducer for maximum temperature example
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// vv MaxTemperatureReducerV1
/**
 * 求每个年份的最高气温
 *
 * <p>Map 端已按年份分组，Reduce 端拿到的 values 就是同一年份的所有温度，
 * 遍历一遍取最大值即可
 * <p>这个 Reducer 同时还被 Driver 用作 Combiner——
 * 同一逻辑在 Map 端先合并一遍，可以显著减少 Shuffle 的数据量
 */
public class MaxTemperatureReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {

  /**
   * 遍历同一键下的所有值，输出其中的最大者
   *
   * @param key 年份
   * @param values 该年份对应的全部温度
   * @param context 上下文，用来写出结果
   * @throws IOException 写出失败
   * @throws InterruptedException 任务被中断
   */
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    
    // 初值取 int 最小值：温度可能是负数，用 0 起步会漏掉全负的情况
    int maxValue = Integer.MIN_VALUE;
    // values 是可迭代对象，框架负责把数据取过来
    for (IntWritable value : values) {
      // get() 取出 int 值；Writable 本身是不可变的，get 不会改变它
      maxValue = Math.max(maxValue, value.get());
    }
    // 每个键最终只写出一条记录
    context.write(key, new IntWritable(maxValue));
  }
}
// ^^ MaxTemperatureReducerV1
