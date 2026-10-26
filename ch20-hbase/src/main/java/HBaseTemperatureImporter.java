import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
   * 用 HBase 的 {@code TableOutputFormat} 把气温数据导入 HBase 表
   *
   * <p>这是三种导入方式里最简单的一种：
   * Mapper 只需要把 {@code Put} 写给框架，
   * 由 {@code TableOutputFormat} 负责真正落到表里
   * <p>行键由 {@link RowKeyConverter} 生成，
   * 列是 data 列族里的 airtemp 限定符
   *
 * Uses HBase's {@link TableOutputFormat} to load temperature data into a HBase table.
 */
public class HBaseTemperatureImporter extends Configured implements Tool {
  
  /**
   * 把 NCDC 记录转成 {@code Put} 的 Mapper
   *
   * <p>泛型参数 {@code K} 是输出键的类型，
   * 这里用不到真正的键值，所以留成泛型
   */
  static class HBaseTemperatureMapper<K> extends Mapper<LongWritable, Text, K, Put> {
    // 解析器复用
    private NcdcRecordParser parser = new NcdcRecordParser();

    /**
     * 处理一行 NCDC 记录
     *
     * @param key 行号（这里用不到）
     * @param value 这一行的文本
     * @param context 作业上下文
     * @throws IOException 解析失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws
        IOException, InterruptedException {
      // 拆成各个字段
      parser.parse(value.toString());
      // 只要温度合格的记录
      if (parser.isValidTemperature()) {
        // 用「站号 + 观测时间」生成行键
        byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(),
            parser.getObservationDate().getTime());
        // 构造这次写入
        Put p = new Put(rowKey);
        // data 列族里的 airtemp 限定符：气温
        p.add(HBaseTemperatureQuery.DATA_COLUMNFAMILY,
            HBaseTemperatureQuery.AIRTEMP_QUALIFIER,
            Bytes.toBytes(parser.getAirTemperature()));
        // 键写 null：行键已经在 Put 里了，输出键用不上
        context.write(null, p);
      }
    }
  }

  /**
   * 作业入口
   *
   * @param args 一个参数：输入目录
   * @return 0 表示成功，-1 表示参数不对
   * @throws Exception 作业执行失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 必须且只需要一个输入目录
    if (args.length != 1) {
      System.err.println("Usage: HBaseTemperatureImporter <input>");
      // 参数不对，直接返回错误码
      return -1;
    }
    // 建立作业，名字用类名
    Job job = new Job(getConf(), getClass().getSimpleName());
    // 告诉作业自己的 jar 在哪儿
    job.setJarByClass(getClass());
    // 指定输入目录
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // 告诉输出格式「往哪张表写」
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "observations");
    // 指定 Mapper
    job.setMapperClass(HBaseTemperatureMapper.class);
    // 不用 Reduce 阶段：只做转换，不需要聚合
    job.setNumReduceTasks(0);
    // 由 HBase 的输出格式负责写表
    job.setOutputFormatClass(TableOutputFormat.class);
    // 提交并等待作业跑完
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * 进程入口
   *
   * @param args 一个参数：输入目录
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会把 HBase 的配置和命令行参数交给这个 Tool
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new HBaseTemperatureImporter(), args);
    // 用退出码告诉外面成功还是失败
    System.exit(exitCode);
  }
}