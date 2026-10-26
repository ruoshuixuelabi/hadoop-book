package oldapi;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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

/**
 * 求最高气温，并把结果写成 Avro 格式（v1 版）
 *
 * <p>Avro 用「模式（Schema）」描述数据，
 * 模式里写清每个字段的名字与类型
 * <p>本例用通用（generic）表示，记录用 {@link GenericRecord} 表示，
 * 字段按名字存取
 * <p>v1 的 Avro 接口用 {@link AvroCollector} 收集结果，
 * 配置与提交都由 {@link JobConf} 和 {@link JobClient} 负责
 */
public class AvroGenericMaxTemperature extends Configured implements Tool {
  
  // 天气记录的模式：年份、气温、站号三个字段
  private static final Schema SCHEMA = new Schema.Parser().parse(
      "{" +
      "  \"type\": \"record\"," +
      "  \"name\": \"WeatherRecord\"," +
      "  \"doc\": \"A weather reading.\"," +
      "  \"fields\": [" +
      "    {\"name\": \"year\", \"type\": \"int\"}," +
      "    {\"name\": \"temperature\", \"type\": \"int\"}," +
      "    {\"name\": \"stationId\", \"type\": \"string\"}" +
      "  ]" +
      "}"
  );

  /**
   * 把每条天气记录转成 Avro 记录并输出
   *
   * <p>输出的键是年份，值是完整的天气记录
   */
  public static class MaxTemperatureMapper
      extends AvroMapper<Utf8, Pair<Integer, GenericRecord>> {
    // 解析器实例：一个 Map 任务只创建一次
    private NcdcRecordParser parser = new NcdcRecordParser();
    // 复用的输出记录：Avro 的记录对象可以反复填字段再写出
    private GenericRecord record = new GenericData.Record(SCHEMA);
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
        AvroCollector<Pair<Integer, GenericRecord>> collector,
        Reporter reporter) throws IOException {
      // 解析这一行记录
      parser.parse(line.toString());
      // 只有读数有效的记录才输出
      if (parser.isValidTemperature()) {
        // 按字段名填值：这里的字段名必须与模式里写的一致
        record.put("year", parser.getYearInt());
        // 温度字段
        record.put("temperature", parser.getAirTemperature());
        // 站号字段
        record.put("stationId", parser.getStationId());
        // 收集器负责把结果交给框架
        // Pair 把键（年份）与值（天气记录）打包在一起
        collector.collect(
            new Pair<Integer, GenericRecord>(parser.getYearInt(), record));
      }
    }
  }
  
  /**
   * 按年份分组求最高气温的 Reducer
   *
   * <p>同一年的所有记录会聚到一个分组里，
   * 逐条比较就能找出气温最高的那条
   */
  public static class MaxTemperatureReducer
      extends AvroReducer<Integer, GenericRecord, GenericRecord> {

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
    public void reduce(Integer key, Iterable<GenericRecord> values,
        AvroCollector<GenericRecord> collector, Reporter reporter)
        throws IOException {
      // 目前还没找到最高气温，所以先留空
      GenericRecord max = null;
      // 遍历本组的所有记录
      for (GenericRecord value : values) {
        // 还没有候选，或者当前这条更热
        // 取出来的字段值要转型成 Integer 才能比较
        if (max == null || 
            (Integer) value.get("temperature") > (Integer) max.get("temperature")) {
          // 记下新的最高气温
          max = newWeatherRecord(value);
        }
      }
      // 输出最高气温对应的那条记录
      collector.collect(max);
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
    private GenericRecord newWeatherRecord(GenericRecord value) {
      // 按同一个模式新建一条记录
      GenericRecord record = new GenericData.Record(SCHEMA);
      // 年份
      record.put("year", value.get("year"));
      // 气温
      record.put("temperature", value.get("temperature"));
      // 站号
      record.put("stationId", value.get("stationId"));
      // 返回复制好的记录
      return record;
    }
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
    // Pair 的模式由键的模式与值的模式拼成
    AvroJob.setMapOutputSchema(conf,
        Pair.getPairSchema(Schema.create(Schema.Type.INT), SCHEMA));
    // 最终输出是天气记录
    AvroJob.setOutputSchema(conf, SCHEMA);
    
    // 输入格式：按 UTF-8 字符串读 Avro 文件
    conf.setInputFormat(AvroUtf8InputFormat.class);

    // 指定 Mapper
    AvroJob.setMapperClass(conf, MaxTemperatureMapper.class);
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
    int exitCode = ToolRunner.run(new AvroGenericMaxTemperature(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
