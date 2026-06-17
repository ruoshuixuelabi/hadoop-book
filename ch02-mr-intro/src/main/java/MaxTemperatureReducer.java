// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MaxTemperatureReducer - 求最大温度的Reducer
 *
 * <p>功能说明：</p>
 * 此Reducer负责汇总来自多个Mapper的输出，计算每个年份的最大温度值。
 *
 * <p>工作流程：</p>
 * <ol>
 *   <li>Hadoop框架按Key（年份）对所有值进行分组</li>
 *   <li>对于每个年份，Reducer收到该年所有温度值的迭代器</li>
 *   <li>遍历所有值，找出最大值</li>
 *   <li>输出(年份, 最高温度)键值对</li>
 * </ol>
 *
 * <p>输入（来自Mapper）：</p>
 * <ul>
 *   <li>Key: Text类型，年份（如"1949"）</li>
 *   <li>Value: IntWritable类型，温度值列表</li>
 * </ul>
 *
 * <p>输出：</p>
 * <ul>
 *   <li>Key: Text类型，年份</li>
 *   <li>Value: IntWritable类型，该年的最高温度</li>
 * </ul>
 *
 * <p>注意：</p>
 * 每个Reducer处理一个年份的数据，如果数据跨多年分布，
 * 则会有多个Reducer实例并行处理不同的年份。
 *
 * @see MaxTemperatureMapper
 * @see MaxTemperature
 */
public class MaxTemperatureReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {

  /**
   * reduce方法 - 找出每个年份的最高温度
   *
   * <p>此方法被Hadoop为每个唯一的Key（即每个年份）调用一次。</p>
   * values迭代器包含该年份所有Mapper输出的温度值，
   * 我们遍历找出最大值并输出。
   *
   * @param key     年份（如"1949"）</li>
   * @param values  该年份所有温度值的迭代器</li>
   * @param context 用于写入最终结果的Context对象</li>
   * @throws IOException 如果发生输入输出错误</li>
   * @throws InterruptedException 如果操作被中断</li>
   */
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {

    // 初始化最大值为整型最小值，确保任何温度值都会比它大
    int maxValue = Integer.MIN_VALUE;

    // 遍历该年份的所有温度值，找出最大值
    for (IntWritable value : values) {
      // 使用Math.max比较当前最大值与新值，更新最大值
      // IntWritable.get()方法返回其包装的int值
      maxValue = Math.max(maxValue, value.get());
    }

    // 输出该年份及其最高温度
    context.write(key, new IntWritable(maxValue));
  }
}
// ^^ MaxTemperatureReducer
