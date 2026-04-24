package v1;
//cc MaxTemperatureReducerV1 Reducer for maximum temperature example

/**
 * 最高温度MapReduce Reducer（版本1）
 *
 * 功能：计算每个年份的最高温度
 *
 * 设计说明：
 * - 输入键：年份（Text类型）
 * - 输入值：该年份的所有温度值（Iterable<IntWritable>）
 * - 输出：(年份, 最高温度)键值对
 *
 * Reducer的工作流程：
 * 1. 遍历所有输入值，找到最大值
 * 2. 输出年份和对应的最高温度
 *
 * Combiner设置说明：
 * 通常将此Reducer同时设置为Combiner，
 * 在Map端进行本地聚合，减少网络传输的数据量
 *
 * @see MaxTemperatureMapper
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// vv MaxTemperatureReducerV1
public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  /**
   * 归约函数：找出每个年份的最高温度
   *
   * @param key     年份（输入键）
   * @param values 该年份的所有温度值迭代器
   * @param context MapReduce上下文
   * @throws IOException          IO异常
   * @throws InterruptedException 中断异常
   */
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    // 初始化最大值为最小整数
    int maxValue = Integer.MIN_VALUE;
    // 遍历所有温度值，找出最大值
    for (IntWritable value : values) {
      maxValue = Math.max(maxValue, value.get());
    }
    // 输出年份和最高温度
    context.write(key, new IntWritable(maxValue));
  }
}
// ^^ MaxTemperatureReducerV1