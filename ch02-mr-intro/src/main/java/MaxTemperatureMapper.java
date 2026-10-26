// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 解析 NCDC 原始记录，输出「年份+站号 → 气温」的键值对
 *
 * <p>Map 阶段只做解析，不做聚合：每读入一行记录就输出一个键值对，
 * 由 Reducer 阶段负责求最大值
 * <p>键用年份而非站号，是因为本例要比较的是「同一站同一年的最高气温」，
 * 而输入文件中的一年数据与下一年数据是分开下载的，若只用站号做键，
 * 不同年份的记录会被混在一起取最大值
 */
public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  /** 缺省温度标记：NCDC 用 9999 表示该时刻无有效读数 */
  private static final int MISSING = 9999;
  
  /**
   * 解析单行 NCDC 记录并输出「年份 → 气温」
   *
   * <p>记录格式为定宽字符串，温度固定占据第 88~92 个字符
   * <p>质量位非「1」表示读数可疑（缺测/可疑/事后校订等），直接丢弃该行
   *
   * @param key    输入的字节偏移量，本例不使用
   * @param value  一行 NCDC 原始记录
   * @param context 作业上下文，用于把结果写出
   * @throws IOException 写出结果时发生 I/O 错误
   * @throws InterruptedException 作业被中断
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
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
      context.write(new Text(year), new IntWritable(airTemperature));
    }
  }
}
// ^^ MaxTemperatureMapper
