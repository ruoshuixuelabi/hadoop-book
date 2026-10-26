// cc OldMaxTemperature Application to find the maximum temperature, using the old MapReduce API
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

// vv OldMaxTemperature
/**
 * 统计每个气象站最高气温的作业入口（MapReduce v1 旧 API，Mapper/Reducer 写为内部类）
 *
 * <p>本类把 Mapper 与 Reducer 定义为静态内部类，与 {@code oldapi} 包中
 * 把它们拆成独立文件的写法不同；两者在功能上等价
 * <p>全部使用 v1 API：{@link JobConf} 承载配置、{@link JobClient} 提交作业、
 * 结果通过 {@link OutputCollector#collect} 写出
 */
public class OldMaxTemperature {
  
  /**
   * 解析 NCDC 原始记录并输出「年份 → 气温」（v1 旧 API）
   *
   * <p>作为静态内部类，编译后是 {@code OldMaxTemperature$OldMaxTemperatureMapper}，
   * 提交作业时需用 {@code OldMaxTemperature.OldMaxTemperatureMapper.class} 引用
   */
  static class OldMaxTemperatureMapper /*[*/extends MapReduceBase/*]*/
    /*[*/implements Mapper/*]*/<LongWritable, Text, Text, IntWritable> {
  
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
    @Override
    public void map(LongWritable key, Text value,
        /*[*/OutputCollector<Text, IntWritable> output, Reporter reporter/*]*/)
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
        /*[*/output.collect/*]*/(new Text(year), new IntWritable(airTemperature));
      }
    }
  }
  
  /**
   * 汇聚同一键下的所有气温并输出最大值（v1 旧 API）
   */
  static class OldMaxTemperatureReducer /*[*/extends MapReduceBase/*]*/
    /*[*/implements Reducer/*]*/<Text, IntWritable, Text, IntWritable> {

    /**
     * 遍历同一键下的所有气温值，取最大者输出
     *
     * @param key     分组键
     * @param values  该键对应的全部气温值；v1 用 Iterator 而非 Iterable
     * @param output  结果收集器，用于把归约结果写出
     * @param reporter 状态上报器
     * @throws IOException 写出结果时发生 I/O 错误
     */
    @Override
    public void reduce(Text key, /*[*/Iterator/*]*/<IntWritable> values,
        /*[*/OutputCollector<Text, IntWritable> output, Reporter reporter/*]*/)
        throws IOException {
      
      // 初值取整型最小值：气温可能为负，用 0 会把负温度错误抬高
      int maxValue = Integer.MIN_VALUE;
      while (/*[*/values.hasNext()/*]*/) {
        maxValue = Math.max(maxValue, /*[*/values.next().get()/*]*/);
      }
      /*[*/output.collect/*]*/(key, new IntWritable(maxValue));
    }
  }

  /**
   * 作业入口：用 v1 风格的 JobConf 组装并提交作业
   *
   * @param args 恰好两个参数：输入路径、输出路径
   * @throws IOException 配置或运行作业时发生 I/O 错误
   */
  public static void main(String[] args) throws IOException {
    // 参数个数校验
    if (args.length != 2) {
      System.err.println("Usage: OldMaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    
    /*[*/JobConf conf = new JobConf(OldMaxTemperature.class);/*]*/
    /*[*/conf/*]*/.setJobName("Max temperature");

    FileInputFormat.addInputPath(/*[*/conf/*]*/, new Path(args[0]));
    FileOutputFormat.setOutputPath(/*[*/conf/*]*/, new Path(args[1]));
    
    // 引用内部类需带上外层类名
    /*[*/conf/*]*/.setMapperClass(OldMaxTemperatureMapper.class);
    /*[*/conf/*]*/.setReducerClass(OldMaxTemperatureReducer.class);

    /*[*/conf/*]*/.setOutputKeyClass(Text.class);
    /*[*/conf/*]*/.setOutputValueClass(IntWritable.class);

    // v1 直接提交并等待作业结束
    /*[*/JobClient.runJob(conf);/*]*/
  }
}
// ^^ OldMaxTemperature