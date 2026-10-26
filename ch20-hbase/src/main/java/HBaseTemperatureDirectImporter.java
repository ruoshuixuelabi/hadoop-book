import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 直接用 {@link HTable} 把气温数据写进 HBase 表
 *
 * <p>这种写法最直白：Mapper 自己拿着表对象往里写
 * <p>但它比 {@link HBaseTemperatureImporter} 效率低，
 * 比 {@link HBaseTemperatureBulkImporter} 效率低得多
 * <p>原因在于 {@code HTable} 默认开启了自动 flush，
 * 每写一小批就往 RegionServer 发一次请求
 * <p>这个例子主要是为了和另外两种写法做对照
 *
 * Uses {@link HTable} to load temperature data into a HBase table directly. Less
 * efficient than {@link HBaseTemperatureImporter} (since auto-flush is enabled) and
 * {@link HBaseTemperatureBulkImporter} in particular.
 */
public class HBaseTemperatureDirectImporter extends Configured implements Tool {
  
  /**
   * 把 NCDC 记录直接写进 HBase 的 Mapper
   *
   * <p>泛型参数 {@code K}、{@code V} 在这里同样用不到
   */
  static class HBaseTemperatureMapper<K, V> extends Mapper<LongWritable, Text, K, V> {
    // 解析器复用
    private NcdcRecordParser parser = new NcdcRecordParser();
    // 表对象，在 setup 里创建，在 cleanup 里关闭
    private HTable table;

    /**
     * 任务开始前的准备工作
     *
     * @param context 作业上下文
     * @throws IOException 建表连接失败
     */
    @Override
    protected void setup(Context context) throws IOException {
      // Create the HBase table client once up-front and keep it around
      // rather than create on each map invocation.
      // 表对象建好之后一直复用，不要每处理一行就新建一个
      // 建连接是有开销的，能省一次是一次
      // 打开 observations 表
      this.table = new HTable(HBaseConfiguration.create(context.getConfiguration()),
          "observations");
    }

    /**
     * 处理一行 NCDC 记录
     *
     * @param key 行号（这里用不到）
     * @param value 这一行的文本
     * @param context 作业上下文
     * @throws IOException 解析或写入失败
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
        // 直接写进 HBase
        table.put(p);
      }
    }

    /**
     * 任务结束后的清理工作
     *
     * @param context 作业上下文
     * @throws IOException 关闭表连接失败
     */
    @Override
    protected void cleanup(Context context) throws IOException {
      // 关掉表连接，别把资源泄漏在集群上
      table.close();
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
      System.err.println("Usage: HBaseTemperatureDirectImporter <input>");
      // 参数不对，直接返回错误码
      return -1;
    }
    // 建立作业，名字用类名
    Job job = new Job(getConf(), getClass().getSimpleName());
    // 告诉作业自己的 jar 在哪儿
    job.setJarByClass(getClass());
    // 指定输入目录
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // 指定 Mapper
    job.setMapperClass(HBaseTemperatureMapper.class);
    // 不用 Reduce 阶段：只做转换，不需要聚合
    job.setNumReduceTasks(0);
    // 数据自己写进 HBase 了，MapReduce 的输出不用要
    job.setOutputFormatClass(NullOutputFormat.class);
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
        new HBaseTemperatureDirectImporter(), args);
    // 用退出码告诉外面成功还是失败
    System.exit(exitCode);
  }
}