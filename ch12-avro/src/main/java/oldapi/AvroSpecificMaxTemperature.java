package oldapi;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroUtf8InputFormat;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import specific.WeatherRecord;

/**
 * 求最高气温，并用「具体记录」读写 Avro（v1 版）
 *
 * <p>与通用（generic）写法相对，Avro 还能由模式生成专门的 Java 类：
 * 这里的 {@code specific.WeatherRecord} 就是由
 * {@code WeatherRecord.avsc} 生成的，字段用 getter/setter 访问
 * <p>好处是编译期就能发现字段名写错，
 * 代价是每种模式都要生成并维护一个类
 */
public class AvroSpecificMaxTemperature extends Configured implements Tool {
  
  /**
   * 把每条天气记录转成具体记录并输出
   *
   * <p>输出的键是年份，值是完整的天气记录
   */
  public static class MaxTemperatureMapper
      extends AvroMapper<Utf8, Pair<Integer, WeatherRecord>> {
    // 解析器实例：一个 Map 任务只创建一次
    private NcdcRecordParser parser = new NcdcRecordParser();
    // 复用的输出记录：Avro 的记录对象可以反复填字段再写出
    private WeatherRecord record = new WeatherRecord();
    /**
     * 解析一行记录并输出「年份 + 天气记录」
     *
     * @param line 输入的一行 NCDC 记录
     * @param collector 结果收集器
     * @param reporter 状态报告器
     * @throws IOException 写出失败
     */
    @Override
    public void map(Utf8 line,
        AvroCollector<Pair<Integer, WeatherRecord>> collector,
        Reporter reporter) throws IOException {
      // 解析这一行记录
      parser.parse(line.toString());
      // 只有读数有效的记录才输出
      if (parser.isValidTemperature()) {
        // 用 setter 填字段，字段名由生成的类保证正确
        record.setYear(parser.getYearInt());
        // 温度字段
        record.setTemperature(parser.getAirTemperature());
        // 站号字段
        record.setStationId(parser.getStationId());
        // 收集器负责把结果交给框架
        // Pair 把键（年份）与值（天气记录）打包在一起
        collector.collect(
            new Pair<Integer, WeatherRecord>(parser.getYearInt(), record));
      }
    }
  }
  
  /**
   * 按年份分组求最高气温的 Reducer
   *
   * <p>具体记录可以直接用 getter 取字段，
   * 不需要像通用写法那样转型
   */
  public static class MaxTemperatureReducer extends
      AvroReducer<Integer, WeatherRecord, WeatherRecord> {

    /**
     * 求本组里的最高气温并输出
     *
     * @param key 分组键，即年份
     * @param values 本组的所有天气记录
     * @param collector 结果收集器
     * @param reporter 状态报告器
     * @throws IOException 写出失败
     */
    @Override
    public void reduce(Integer key, Iterable<WeatherRecord> values,
        AvroCollector<WeatherRecord> collector,
        Reporter reporter) throws IOException {
      // 目前还没找到最高气温，所以先留空
      WeatherRecord max = null;
      // 遍历本组的所有记录
      for (WeatherRecord value : values) {
        // 还没有候选，或者当前这条更热
        if (max == null || value.getTemperature() > max.getTemperature()) {
          // 记下新的最高气温
          max = newWeatherRecord(value);
        }
      }
      // 输出最高气温对应的那条记录
      collector.collect(max);
    }
  }

  /**
   * 在 Map 端先做一次归并的 Combiner
   *
   * <p>Combiner 跑在 Map 输出之后、Reduce 之前，
   * 它的输出格式必须与 Map 的输出一致，
   * 所以这里要再打包回 Pair
   */
  public static class MaxTemperatureCombiner extends
      AvroReducer<Integer, WeatherRecord, Pair<Integer, WeatherRecord>> {
    
    /**
     * 求本组里的最高气温并输出
     *
     * @param key 分组键，即年份
     * @param values 本组的所有天气记录
     * @param collector 结果收集器
     * @param reporter 状态报告器
     * @throws IOException 写出失败
     */
    @Override
    public void reduce(Integer key, Iterable<WeatherRecord> values,
        AvroCollector<Pair<Integer, WeatherRecord>> collector,
        Reporter reporter) throws IOException {
      // 目前还没找到最高气温，所以先留空
      WeatherRecord max = null;
      // 遍历本组的所有记录
      for (WeatherRecord value : values) {
        // 还没有候选，或者当前这条更热
        if (max == null || value.getTemperature() > max.getTemperature()) {
          // 记下新的最高气温
          max = newWeatherRecord(value);
        }
      }
      // 再打包成 Pair，与 Map 的输出格式保持一致
      collector.collect(new Pair<Integer, WeatherRecord>(key, max));
    }
  }

  /**
   * 复制出一条新的天气记录
   *
   * <p>Avro 的记录对象在读出之后可以被复用，
   * 所以这里复制一份保存下来，避免后面的记录覆盖它
   *
   * @param value 要复制的记录
   * @return 复制得到的新记录
   */
  private static WeatherRecord newWeatherRecord(WeatherRecord value) {
    // 生成类提供了按字段复制构造的构造函数
    return new WeatherRecord(value.getYear(), value.getTemperature(), value.getStationId());
  }
  
  /**
   * 运行作业
   *
   * @param args 依次为输入路径与输出路径
   * @return 0 表示成功，-1 表示参数错误
   * @throws Exception 作业执行失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 需要一个输入路径加一个输出路径
    if (args.length != 2) {
      // 打印用法提示
      // 第二个参数是类名
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      // 再说明一遍通用选项
      ToolRunner.printGenericCommandUsage(System.err);
      // 参数错误，返回 -1
      return -1;
    }
    
    // v1 用 JobConf 承载全部配置
    JobConf conf = new JobConf(getConf(), getClass());
    // 设置作业名
    conf.setJobName("Max temperature");
    
    // 输入路径
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    // 输出路径
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    // 输入是 Avro 的字符串类型（每行一条记录）
    AvroJob.setInputSchema(conf, Schema.create(Schema.Type.STRING));
    // Map 输出是「年份 + 天气记录」这一对
    // 键用 int 模式，值用生成类的模式
    AvroJob.setMapOutputSchema(conf, Pair.getPairSchema(
        Schema.create(Schema.Type.INT), WeatherRecord.SCHEMA$));
    // 最终输出是天气记录
    AvroJob.setOutputSchema(conf, WeatherRecord.SCHEMA$);
    
    // 输入格式：按 UTF-8 字符串读 Avro 文件
    conf.setInputFormat(AvroUtf8InputFormat.class);

    // 指定 Mapper
    AvroJob.setMapperClass(conf, MaxTemperatureMapper.class);
    // 指定 Combiner：在 Map 端先做一次归并，减少网络传输量
    AvroJob.setCombinerClass(conf, MaxTemperatureCombiner.class);
    // 指定 Reducer
    AvroJob.setReducerClass(conf, MaxTemperatureReducer.class);

    // v1 由 JobClient 提交并运行作业
    JobClient.runJob(conf);
    // 运行成功
    return 0;
  }
  
  /**
   * 程序入口
   *
   * @param args 命令行参数
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会解析通用选项并调用 run
    int exitCode = ToolRunner.run(new AvroSpecificMaxTemperature(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
