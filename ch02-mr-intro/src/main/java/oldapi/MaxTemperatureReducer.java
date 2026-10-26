package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * 汇聚同一键下的所有气温并输出最大值（MapReduce v1 旧 API 版本）
 *
 * <p>与默认包中 {@code MaxTemperatureReducer} 的差别在于 v1 的接口约定：
 * 需要显式 {@code extends MapReduceBase} 并 {@code implements Reducer}，
 * 写出结果用 {@link OutputCollector#collect} 而非 v2 的 {@code context.write}
 */
public class MaxTemperatureReducer extends MapReduceBase
  implements Reducer<Text, IntWritable, Text, IntWritable> {

  /**
   * 遍历同一键下的所有气温值，取最大者输出
   *
   * @param key    分组键
   * @param values 该键对应的全部气温值；v1 用 Iterator 而非 Iterable
   * @param output 结果收集器，用于把归约结果写出
   * @param reporter 状态上报器；v1 的 Mapper/Reducer 可用它报告进度与错误
   * @throws IOException 写出结果时发生 I/O 错误
   */
  public void reduce(Text key, Iterator<IntWritable> values,
      OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {
    
    // 初值取整型最小值：气温可能为负，用 0 会把负温度错误抬高
    int maxValue = Integer.MIN_VALUE;
    while (values.hasNext()) {
      maxValue = Math.max(maxValue, values.next().get());
    }
    output.collect(key, new IntWritable(maxValue));
  }
}
