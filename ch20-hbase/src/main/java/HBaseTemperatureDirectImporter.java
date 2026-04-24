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
 * 直接使用 {@link HTable} 将温度数据加载到 HBase 表中。
 *
 * 此方式是三种导入方式中最简单但效率最低的:
 * 1. 使用 HBaseTemperatureDirectImporter (本类): 直接 put,自动 flush 开启,每个 put 立即刷新
 * 2. 使用 HBaseTemperatureImporter: 通过 TableOutputFormat 批量处理
 * 3. 使用 HBaseTemperatureBulkImporter: 批量加载 HFile,最高效
 *
 * 缺点:
 * - 开启 auto-flush,每个 put 后立即刷新到服务器
 * - 网络往返开销大,不适合大规模数据导入
 * - 未利用 MapReduce 的并行处理能力
 *
 * /*[*
 * auto-flush 开启意味着每次 put 后,HTable 会立即将数据发送到 RegionServer,
 * 而不是等到缓冲区满才发送。这保证了数据的实时性,但牺牲了性能。
 * 对于小数据量或实时性要求高的场景可以使用此方式。
 * *]*/
public class HBaseTemperatureDirectImporter extends Configured implements Tool {

  /**
   * Mapper: 在 map 方法中直接使用 HTable 执行 put 操作
   *
   * 与其他 MR 作业不同,此 Mapper 在 map 方法中直接与 HBase 交互,
   * 而非输出到 context 由 OutputFormat 处理。
   *
   * 关键设计点:
   * - 在 setup() 中创建 HTable,整个作业期间复用
   * - map() 中直接调用 table.put() 写入数据
   * - 在 cleanup() 中关闭 HTable
   *
   * @param <K> 输入 key 类型
   * @param <V> 输入 value 类型
   */
  static class HBaseTemperatureMapper<K, V> extends Mapper<LongWritable, Text, K, V> {
    // 解析器用于提取气象数据中的站点ID、观测时间、温度
    private NcdcRecordParser parser = new NcdcRecordParser();
    // HTable 实例,整个作业期间复用,避免重复创建开销
    private HTable table;

    /**
     * setup 阶段: 创建 HTable 连接
     *
     * setup() 在每个 map task 启动时调用一次,适合执行初始化操作。
     * 此处创建的 HTable 会被所有 map() 调用共享,直到 cleanup() 被调用。
     *
     * 注意: HTable 不是线程安全的,不应在多线程环境中共享
     */
    @Override
    protected void setup(Context context) throws IOException {
      // 在 map task 启动时创建 HTable 连接
      // HBaseConfiguration.create(context.getConfiguration()) 使用 job 配置创建配置
      // "observations" 是目标表名
      this.table = new HTable(HBaseConfiguration.create(context.getConfiguration()),
          "observations");
      // HTable 构造函数中 auto-flush 默认开启
      // 如需关闭可调用: table.setAutoFlush(false)
    }

    /**
     * map 方法: 解析气象数据并写入 HBase
     *
     * @param key 行偏移量
     * @param value 原始气象数据行
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws
        IOException, InterruptedException {
      // 解析原始数据
      parser.parse(value.toString());

      // 只处理有效温度记录
      if (parser.isValidTemperature()) {
        // 生成行键: 站点ID(12字节) + 逆序时间戳(8字节)
        byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(),
            parser.getObservationDate().getTime());

        // 创建 Put 对象
        Put p = new Put(rowKey);
        // 添加列: 列族 "data", 列名 "airtemp", 值为温度
        p.add(HBaseTemperatureQuery.DATA_COLUMNFAMILY,
            HBaseTemperatureQuery.AIRTEMP_QUALIFIER,
            Bytes.toBytes(parser.getAirTemperature()));

        // 直接写入 HBase
        // 由于 auto-flush 开启,此 put 会立即发送到 RegionServer
        table.put(p);
      }
    }

    /**
     * cleanup 阶段: 关闭 HTable 连接
     *
     * cleanup() 在 map task 完成时调用,用于释放资源。
     * 确保 HTable 被正确关闭,避免连接泄漏。
     */
    @Override
    protected void cleanup(Context context) throws IOException {
      table.close();
    }
  }

  /**
   * 配置并运行 MR 作业
   *
   * 配置要点:
   * - 不使用 Reducer (setNumReduceTasks(0))
   * - 使用 NullOutputFormat,因为输出不是文件而是 HBase 表
   * - Mapper 负责所有数据写入工作
   *
   * @param args 命令行参数, args[0] 为输入路径
   * @return 作业执行状态
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: HBaseTemperatureDirectImporter <input>");
      return -1;
    }
    Job job = new Job(getConf(), getClass().getSimpleName());
    job.setJarByClass(getClass());

    // 配置输入路径
    FileInputFormat.addInputPath(job, new Path(args[0]));

    // 设置 Mapper 类
    job.setMapperClass(HBaseTemperatureMapper.class);

    // 无 Reducer,数据由 Mapper 直接写入 HBase
    job.setNumReduceTasks(0);

    // 使用 NullOutputFormat,因为输出目标是 HBase 表而非文件
    // 此设置避免 MR 尝试写入文件系统
    job.setOutputFormatClass(NullOutputFormat.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * 程序入口
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new HBaseTemperatureDirectImporter(), args);
    System.exit(exitCode);
  }
}