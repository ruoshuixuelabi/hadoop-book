// cc AvroGenericMaxTemperature MapReduce program to find the maximum temperature, creating Avro output

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//vv AvroGenericMaxTemperature
/**
 * 求最高气温，并把结果写成 Avro 格式
 *
 * <p>Avro 用「模式（Schema）」描述数据：
 * 模式里写清每个字段的名字与类型，读写两端都靠它来解释字节
 * <p>本例用的是 Avro 的通用（generic）表示：
 * 记录用 {@link GenericRecord} 表示，字段按名字存取，
 * 不必为每种模式生成专门的 Java 类
 * <p>Map 输出的键是 {@link AvroKey}（包装年份），
 * 值是 {@link AvroValue}（包装一条天气记录）
 */
public class AvroGenericMaxTemperature extends Configured implements Tool {
  
  // 天气记录的模式：年份、气温、站号三个字段
  // 模式写成一个 JSON 字符串，交给 Avro 的解析器处理
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
   * <p>输出的键是年份，值是完整的天气记录，
   * 于是 Reducer 端按年份分组后可以逐条比较气温
   */
  public static class MaxTemperatureMapper
      extends Mapper<LongWritable, Text, AvroKey<Integer>,
            AvroValue<GenericRecord>> {
    // 解析器实例：一个 Map 任务只创建一次
    private NcdcRecordParser parser = new NcdcRecordParser();
    // 复用的输出记录：Avro 的记录对象可以反复填字段再写出
    private GenericRecord record = new GenericData.Record(SCHEMA);

    /**
     * 解析一行记录并输出「年份 + 天气记录」
     *
     * @param key 输入的键（本例用不到）
     * @param value 输入的值，即一行 NCDC 记录
     * @param context 上下文，用来写出结果
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      // 解析这一行记录
      parser.parse(value.toString());
      // 只有读数有效的记录才输出
      if (parser.isValidTemperature()) {
        // 按字段名填值：这里的字段名必须与模式里写的一致
        record.put("year", parser.getYearInt());
        // 温度字段
        record.put("temperature", parser.getAirTemperature());
        // 站号字段
        record.put("stationId", parser.getStationId());
        // 键是年份，AvroKey 把它包装成 Avro 的 int 类型
        // 值是刚才填好的天气记录
        context.write(new AvroKey<Integer>(parser.getYearInt()),
            new AvroValue<GenericRecord>(record));
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
      extends Reducer<AvroKey<Integer>, AvroValue<GenericRecord>,
            AvroKey<GenericRecord>, NullWritable> {

    /**
     * 求本组里的最高气温并输出
     *
     * @param key 分组键，即年份
     * @param values 本组的所有天气记录
     * @param context 上下文，用来写出结果
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<GenericRecord>>
        values, Context context) throws IOException, InterruptedException {
      // 目前还没找到最高气温，所以先留空
      GenericRecord max = null;
      // 遍历本组的所有记录
      for (AvroValue<GenericRecord> value : values) {
        // 从 AvroValue 里取出真正的记录对象
        GenericRecord record = value.datum();
        // 还没有候选，或者当前这条更热
        // 取出来的字段值要转型成 Integer 才能比较
        if (max == null || 
            (Integer) record.get("temperature") > (Integer) max.get("temperature")) {
          // 记下新的最高气温
          max = newWeatherRecord(record);
        }
      }
      // 输出最高气温对应的那条记录，值没有意义所以用 NullWritable
      context.write(new AvroKey(max), NullWritable.get());
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

    // 新建作业
    Job job = new Job(getConf(), "Max temperature");
    // 告诉框架作业的 jar 包在哪里
    job.setJarByClass(getClass());

    // 打开这个开关，作业才会优先使用用户 jar 包里的类
    // Avro 的读写类必须来自作业自己的 jar 包
    job.getConfiguration().setBoolean(
        Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

    // 输入路径
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // 输出路径
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Map 输出的键是 Avro 的 int 类型（年份）
    AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
    // Map 输出的值是天气记录
    AvroJob.setMapOutputValueSchema(job, SCHEMA);
    // 最终输出的键也是天气记录
    AvroJob.setOutputKeySchema(job, SCHEMA);

    // 输入是普通文本
    job.setInputFormatClass(TextInputFormat.class);
    // 输出是 Avro 格式
    job.setOutputFormatClass(AvroKeyOutputFormat.class);

    // 指定 Mapper
    job.setMapperClass(MaxTemperatureMapper.class);
    // 指定 Reducer
    job.setReducerClass(MaxTemperatureReducer.class);

    // 提交作业并等待它跑完
    return job.waitForCompletion(true) ? 0 : 1;
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
// ^^ AvroGenericMaxTemperature
