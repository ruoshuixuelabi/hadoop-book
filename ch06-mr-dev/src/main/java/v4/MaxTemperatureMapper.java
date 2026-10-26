package v4;
// cc MaxTemperatureMapperV4 Mapper for maximum temperature example
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// vv MaxTemperatureMapperV4
/**
 * 第四版 Mapper：区分「数据缺失」和「数据损坏」
 *
 * <p>v3 把所有可疑记录一视同仁地计数，
 * 但「温度是 9999（合法地表示缺失）」和「温度字段根本不是数字」
 * 是两类不同的问题
 * <p>v4 让解析器额外报告「格式损坏」，
 * Mapper 只对真正损坏的记录打日志和计数
 */
public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  
  // 计数器分组：只统计格式损坏的记录
  enum Temperature {
    MALFORMED
  }

  // 解析器复用一个实例即可
  private NcdcRecordParser parser = new NcdcRecordParser();
  
  /**
   * 解析记录：有效的输出，损坏的记日志并计数
   *
   * @param key 输入的键（此处用不到）
   * @param value 输入的值，即一整行记录
   * @param context 上下文，用来写出结果
   * @throws IOException 写出失败
   * @throws InterruptedException 任务被中断
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    // 先解析出年份、气温和质量标记
    parser.parse(value);
    // 温度可信，正常输出
    if (parser.isValidTemperature()) {
      int airTemperature = parser.getAirTemperature();
      // 有效记录照常进入结果
      context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
    // 温度字段既不是数字也不是缺失标记，说明这条记录本身坏了
    } else if (parser.isMalformedTemperature()) {
      // 把坏记录打进日志，便于回溯原始数据
      System.err.println("Ignoring possibly corrupt input: " + value);
      // 计数器只统计真正损坏的记录
      context.getCounter(Temperature.MALFORMED).increment(1);
    }
  }
}
// ^^ MaxTemperatureMapperV4
