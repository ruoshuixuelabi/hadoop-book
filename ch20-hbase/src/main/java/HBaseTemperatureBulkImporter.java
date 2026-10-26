import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
   * 用 HBase 的批量导入功能把气温数据高效地装进 HBase 表
   *
   * <p>这是三种导入方式里效率最高的一种：
   * Mapper 并不直接写 HBase，
   * 而是先把数据写成 HFile，再由 {@code LoadIncrementalHFiles} 一次性装载进去
   * <p>这样避免了成千上万次小写入，
   * 是把大批量数据灌进 HBase 的标准做法
   *
 * Uses HBase's bulk load facility ({@link HFileOutputFormat2} and {@link
 * LoadIncrementalHFiles}) to efficiently load temperature data into a HBase table.
 */
public class HBaseTemperatureBulkImporter extends Configured implements Tool {
  
  /**
   * 把 NCDC 记录转成 HFile 里的一条记录的 Mapper
   *
   * <p>输出的键是行键，值是 {@code Put}，
   * 这正是 {@code HFileOutputFormat2} 要求的格式
   */
  static class HBaseTemperatureMapper extends Mapper<LongWritable, Text,
      ImmutableBytesWritable, Put> {
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
        // 键是行键，值是这条 Put
        // 输出格式会拿键当作 HFile 里的行键
        context.write(new ImmutableBytesWritable(rowKey), p);
      }
    }
  }

  /**
   * 作业入口
   *
   * @param args 一个参数：输入目录
   * @return 0 表示成功，1 表示作业失败，-1 表示参数不对
   * @throws Exception 作业执行失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 必须且只需要一个输入目录
    if (args.length != 1) {
      System.err.println("Usage: HBaseTemperatureBulkImporter <input>");
      // 参数不对，直接返回错误码
      return -1;
    }
    // 把 HBase 的配置合进来
    Configuration conf = HBaseConfiguration.create(getConf());
    // 建立作业，名字用类名
    Job job = new Job(conf, getClass().getSimpleName());
    // 告诉作业自己的 jar 在哪儿
    job.setJarByClass(getClass());
    // 指定输入目录
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // 中间 HFile 的存放目录
    // 批量导入要先把数据写成 HFile，再整体装载
    Path tmpPath = new Path("/tmp/bulk");
    // 把作业的输出目录设成这个临时目录
    FileOutputFormat.setOutputPath(job, tmpPath);
    // 指定 Mapper
    job.setMapperClass(HBaseTemperatureMapper.class);
    // 输出的键类型
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    // 输出的值类型
    job.setMapOutputValueClass(Put.class);
    // 打开要装载进去的表
    HTable table = new HTable(conf, "observations");
    // 后面无论成功失败都要关掉表，所以放在 try 里
    try {
      // 关键的一步：让输出格式按「增量装载」的方式写 HFile
      // 它会把 Region 的分界等信息也写进去，方便后面快速装载
      HFileOutputFormat2.configureIncrementalLoad(job, table);

      // 等待作业跑完
      if (!job.waitForCompletion(true)) {
        // 作业失败
        return 1;
      }

      // 批量装载器
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
      // 把 HFile 一次性装载进表里
      // 这一步是原子的，比一条条写快得多
      loader.doBulkLoad(tmpPath, table);
      // 装载完就把中间文件删掉，别占着空间
      FileSystem.get(conf).delete(tmpPath, true);
      // 全部成功
      return 0;
    // 无论正常结束还是抛异常，都要收拾干净
    } finally {
      // 关闭表连接，释放资源
      table.close();
    }
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
        new HBaseTemperatureBulkImporter(), args);
    // 用退出码告诉外面成功还是失败
    System.exit(exitCode);
  }
}