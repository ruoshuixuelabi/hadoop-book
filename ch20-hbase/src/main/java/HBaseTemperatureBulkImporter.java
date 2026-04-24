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
 * 使用 HBase 的批量加载功能 ({@link HFileOutputFormat2} 和 {@link LoadIncrementalHFiles})
 * 高效地将温度数据加载到 HBase 表中。
 *
 * 与 {@link HBaseTemperatureDirectImporter} 直接写入方式相比,
 * 批量加载方式绕过 Write-Ahead Log (WAL), 直接将 HFile 写入 RegionServer,
 * 大幅提升数据导入性能,尤其适合大规模数据导入场景。
 *
 * 流程概述:
 * 1. Mapper 将数据转换为 Put 对象,输出为 ImmutableBytesWritable (rowkey) -> Put
 * 2. HFileOutputFormat2 将 mapper 输出组织成 HBase 的存储格式 (HFile)
 * 3. LoadIncrementalHFiles 将生成的 HFile 文件加载到 HBase 表中
 *
 * Bulk load 方式避免了直接写入 WAL,适合离线大批量数据导入
 */
public class HBaseTemperatureBulkImporter extends Configured implements Tool {

  /**
   * Mapper 组件: 将解析后的气象记录转换为 HBase Put 操作
   *
   * 输入: LongWritable (行偏移) -> Text (原始气象数据行)
   * 输出: ImmutableBytesWritable (行键) -> Put (HBase 写入操作)
   *
   * 注意: 输出格式由 HFileOutputFormat2 控制,用于生成 HBase 内部存储文件
   */
  static class HBaseTemperatureMapper extends Mapper<LongWritable, Text,
      ImmutableBytesWritable, Put> {
    // 解析器用于从原始气象数据中提取站点ID、观测时间、温度等信息
    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    public void map(LongWritable key, Text value, Context context) throws
        IOException, InterruptedException {
      // 解析气象数据行
      parser.parse(value.toString());
      // 仅处理有效温度记录,过滤无效数据(如缺失温度值的记录)
      if (parser.isValidTemperature()) {
        // 将站点ID和观测时间组合成 HBase 行键
        // RowKey 格式: <station_id><reverse_timestamp>
        byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(),
            parser.getObservationDate().getTime());
        // 创建 Put 对象,指定行键
        Put p = new Put(rowKey);
        // 添加列: 列族为 "data", 列名为 "airtemp", 值为温度值
        // Bytes.toBytes() 将 Java 基本类型转换为 HBase 内部使用的字节数组
        p.add(HBaseTemperatureQuery.DATA_COLUMNFAMILY,
            HBaseTemperatureQuery.AIRTEMP_QUALIFIER,
            Bytes.toBytes(parser.getAirTemperature()));
        // 将 Put 对象写入 context,供 HFileOutputFormat2 处理
        // ImmutableBytesWritable 包装 rowKey 作为输出 key
        context.write(new ImmutableBytesWritable(rowKey), p);
      }
    }
  }

  /**
   * 作业配置与执行入口
   *
   * 配置步骤:
   * 1. 创建 Job 并设置基本信息(名称、jar类)
   * 2. 配置输入输出路径
   * 3. 设置 Mapper 类及其输入输出类型
   * 4. 使用 HFileOutputFormat2.configureIncrementalLoad() 自动配置分区和排序
   * 5. 执行 job 后使用 LoadIncrementalHFiles 完成批量加载
   *
   * @param args 命令行参数, args[0] 为输入数据路径
   * @return 作业执行状态 (0 成功, 1 失败)
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: HBaseTemperatureBulkImporter <input>");
      return -1;
    }
    // 创建 HBase 配置,合并 Hadoop 和 HBase 配置
    Configuration conf = HBaseConfiguration.create(getConf());
    Job job = new Job(conf, getClass().getSimpleName());
    job.setJarByClass(getClass());

    // 配置输入路径: 从命令行参数指定的目录读取气象数据文件
    FileInputFormat.addInputPath(job, new Path(args[0]));

    // 设置临时输出路径,用于存储生成的 HFile
    // 注意: 此路径应为 HDFS 上的路径,以便 RegionServer 访问
    Path tmpPath = new Path("/tmp/bulk");
    FileOutputFormat.setOutputPath(job, tmpPath);

    // 设置 Mapper 类
    job.setMapperClass(HBaseTemperatureMapper.class);
    // 指定 Mapper 输出类型: key 为 ImmutableBytesWritable (包装 rowkey), value 为 Put
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);

    // 创建 HTable 实例,用于获取表信息和配置分区
    HTable table = new HTable(conf, "observations");
    try {
      // 自动配置 incremental load:
      // 1. 根据 table 的 Region 分布设置 partitioner
      // 2. 配置输出格式为 HFileOutputFormat2
      // 3. 确保输出按键(行键)排序,符合 HFile 要求
      HFileOutputFormat2.configureIncrementalLoad(job, table);

      // 等待 MapReduce 作业完成
      if (!job.waitForCompletion(true)) {
        return 1;
      }

      /*[*
       * 执行批量加载 (Bulk Load):
       * 此方法将 /tmp/bulk 目录下的 HFile 直接加载到表中,
       * 绕过 WAL 直接写入 StoreFile,性能远高于直接 put
       *]*/
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
      loader.doBulkLoad(tmpPath, table);

      // 清理临时目录
      FileSystem.get(conf).delete(tmpPath, true);
      return 0;
    } finally {
      // 确保 HTable 连接关闭,释放资源
      table.close();
    }
  }

  /**
   * 程序入口: 使用 ToolRunner 运行此工具
   *
   * ToolRunner 负责:
   * 1. 解析命令行参数
   * 2. 配置 Hadoop/HBase 配置
   * 3. 调用 run() 方法执行作业
   * 4. 返回退出码给操作系统
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new HBaseTemperatureBulkImporter(), args);
    System.exit(exitCode);
  }
}