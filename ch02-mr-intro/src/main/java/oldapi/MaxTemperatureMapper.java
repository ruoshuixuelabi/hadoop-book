package oldapi;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 解析 NCDC 原始记录并输出「年份 → 气温」（MapReduce v1 旧 API 版本）
 *
 * <p>解析逻辑与默认包中 {@code MaxTemperatureMapper} 完全一致，
 * 差别只在 v1 的接口约定：结果通过 {@link OutputCollector#collect} 写出，
 * 而不是 v2 的 {@code context.write}
 */
public class MaxTemperatureMapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {

  /** 缺省温度标记：NCDC 用 9999 表示该时刻无有效读数 */
  private static final int MISSING = 9999;
  
  /**
   * 解析单行 NCDC 记录并输出「年份 → 气温」
   *
   * @param key     输入的字节偏移量，本例不使用
   * @param value   一行 NCDC 原始记录
   * @param output  结果收集器，用于把键值对写出
   * @param reporter 状态上报器
   * @throws IOException 写出结果时发生 I/O 错误
   */
  public void map(LongWritable key, Text value,
      OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {
    
    String line = value.toString();
    // 年份位于第 16~19 个字符，作为分组的组成部分
    String year = line.substring(15, 19);
    int airTemperature;
    // 温度可能带正号前缀，而 Integer.parseInt 不接受 "+1" 形式的正号
    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
    // 质量位紧跟温度之后，用来筛掉不可信的读数
    String quality = line.substring(92, 93);
    // 只输出有效读数：温度不缺测，且质量位属于「已校订/良好/合格」之一
    if (airTemperature != MISSING && quality.matches("[01459]")) {
      output.collect(new Text(year), new IntWritable(airTemperature));
    }
  }
}
