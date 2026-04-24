package v1;
// cc MaxTemperatureMapperV1 First version of a Mapper that passes MaxTemperatureMapperTest

/**
 * 最高温度MapReduce Mapper（版本1）
 *
 * 这是最基础的Mapper版本，直接解析NCDC气象数据记录。
 *
 * 功能说明：
 * - 输入：NCDC格式的原始气象数据（固定宽度格式）
 * - 输出：(年份, 温度)键值对
 *
 * NCDC记录格式（本版本关注的位置）：
 * - 位置15-18：年份（4位数字）
 * - 位置87-92：温度值（5位数字，可能包含前导加号）
 *
 * 注意：
 * - 本版本不处理缺失温度值（9999）
 * - 本版本不处理格式错误的温度记录
 * - 本版本不进行任何数据验证
 *
 * 此版本演示了最基本的Mapper实现，
 * 后续版本（v2、v3、v4）逐步增加了数据验证和错误处理能力
 *
 * @see MaxTemperatureReducer
 * @see MaxTemperatureMapperTest
 */
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

//vv MaxTemperatureMapperV1
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  /**
   * 解析NCDC记录并提取年份和温度
   *
   * @param key     输入分片的行偏移量（LongWritable类型）
   * @param value   NCDC格式的原始记录字符串
   * @param context MapReduce上下文，用于输出键值对
   * @throws IOException          IO异常
   * @throws InterruptedException 中断异常
   */
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    // 将Text对象转换为字符串
    String line = value.toString();
    // 从位置15-18提取4位年份
    String year = line.substring(15, 19);
    // 从位置87-92提取5位温度值
    int airTemperature = Integer.parseInt(line.substring(87, 92));
    // 输出年份-温度键值对
    context.write(new Text(year), new IntWritable(airTemperature));
  }
}
//^^ MaxTemperatureMapperV1