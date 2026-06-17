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
 * 旧API Reducer类: 计算每个年份的最高气温.
 *
 * <p>Reduce任务接收Mapper输出的&lt;年份, 气温&gt;键值对,
 * 对同一年份的所有气温值进行聚合,找出最大值作为该年份的最高气温.
 *
 * <p>处理流程:
 * <ul>
 *   <li>遍历所有气温值,维护当前最大值</li>
 *   <li>输出&lt;年份, 年度最高气温&gt;</li>
 * </ul>
 *
 * <p>注意: Hadoop会对键值对按key(年份)排序,因此Reduce接收的
 * values已经是该年份所有气温值的集合.
 *
 * @see MaxTemperatureMapper
 */
public class MaxTemperatureReducer extends MapReduceBase
  implements Reducer<Text, IntWritable, Text, IntWritable> {

  /**
   * Reduce任务的核心处理方法,计算一组气温值中的最大值.
   *
   * <p>对于每个年份(键),遍历其对应的所有气温观测值,
   * 使用Math.max()比较找出最高气温,然后输出该年份的最高气温记录.
   *
   * <p>处理逻辑:
   * <ol>
   *   <li>初始化maxValue为Integer.MIN_VALUE(最小整数值)</li>
   *   <li>遍历values迭代器,逐个比较取最大值</li>
   *   <li>输出&lt;年份, 最高气温&gt;键值对</li>
   * </ol>
   *
   * <p>设计说明: 使用Integer.MIN_VALUE初始化是因为实际气温值
   * 必然大于此值,确保第一个观测值会覆盖初始值.
   *
   * @param key 年份字符串(如"1949", "1950"等)
   * @param values 该年份的所有气温观测值迭代器
   * @param output 收集结果的输出器
   * @param reporter 作业Reporter,用于报告进度和状态
   * @throws IOException 如果输出时发生IO错误
   */
  public void reduce(Text key, Iterator<IntWritable> values,
      OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {

    // 初始化为最小整数值,确保第一个气温值会更新它
    int maxValue = Integer.MIN_VALUE;

    // 遍历该年份的所有气温观测值,找出最大值
    while (values.hasNext()) {
      // values.next().get()获取IntWritable包装的整数值
      maxValue = Math.max(maxValue, values.next().get());
    }

    // 输出该年份的最高气温
    output.collect(key, new IntWritable(maxValue));
  }
}