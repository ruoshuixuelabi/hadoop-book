/**
 * 文件：HBaseTemperatureBulkImporter.java<br>
 * 路径：src/main/java/HBaseTemperatureBulkImporter.java<br>
 * 描述：使用 HBase 的批量加载功能高效地将温度数据加载到 HBase 表<br>
 *<br>
 * 功能说明：<br>
 * 使用 HBase 的 HFileOutputFormat2 和 LoadIncrementalHFiles 进行批量加载，<br>
 * 高效地将气象温度数据导入 HBase 表。<br>
 *<br>
 * 与直接写入方式相比：<br>
 * - 批量加载方式绕过 Write-Ahead Log (WAL)，<br>
 * - 直接将 HFile 写入 RegionServer，<br>
 * - 大幅提升数据导入性能，特别适合大规模数据导入场景。<br>
 *<br>
 * 流程概述：<br>
 * 1. Mapper 将数据转换为 Put 对象，输出为 ImmutableBytesWritable (rowkey) -> Put<br>
 * 2. HFileOutputFormat2 将 mapper 输出组织成 HBase 的存储格式（HFile）<br>
 * 3. LoadIncrementalHFiles 将生成的 HFile 文件加载到 HBase 表中<br>
 *<br>
 * HBase 2.x API 变化：<br>
 * - configureIncrementalLoad() 需要额外的 RegionLocator 参数<br>
 * - doBulkLoad() 需要 Admin 和 RegionLocator 参数<br>
 *<br>
 * 运行命令：<br>
 * hbase HBaseTemperatureBulkImporter <输入数据路径><br>
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
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
 * 温度数据批量导入器<br>
 * 使用 HBase 批量加载功能高效导入气象温度数据。<br>
 */
public class HBaseTemperatureBulkImporter extends Configured implements Tool {

    /**
     * 温度数据 Mapper 类<br>
     * 将解析后的气象记录转换为 HBase Put 操作。<br>
     *<br>
     * 输入：LongWritable（行偏移）-> Text（原始气象数据行）<br>
     * 输出：ImmutableBytesWritable（行键）-> Put（HBase 写入操作）<br>
     *<br>
     * 注意：输出格式由 HFileOutputFormat2 控制，用于生成 HBase 内部存储文件。<br>
     */
    static class HBaseTemperatureMapper extends Mapper<LongWritable, Text,
            ImmutableBytesWritable, Put> {
        // 解析器用于从原始气象数据中提取站点ID、观测时间、温度等信息
        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * 地图处理方法<br>
         * 解析气象数据行，过滤无效记录，并转换为 HBase Put 操作。<br>
         *<br>
         * @param key 行偏移量（输入数据的行号）<br>
         * @param value 一行气象数据文本<br>
         * @param context MapReduce 上下文<br>
         * @throws IOException 如果发生 I/O 错误<br>
         * @throws InterruptedException 如果处理被中断<br>
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws
                IOException, InterruptedException {
            // 解析气象数据行
            // parser.parse() 会提取站点ID、观测时间、气温等字段
            parser.parse(value.toString());

            // 仅处理有效温度记录，过滤无效数据（如缺失温度值的记录）
            if (parser.isValidTemperature()) {
                // 将站点ID和观测时间组合成 HBase 行键
                // RowKey 格式: <station_id><reverse_timestamp>
                byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(),
                        parser.getObservationDate().getTime());

                // 创建 Put 对象，指定行键
                // Put 表示一次 HBase 写入操作
                Put p = new Put(rowKey);

                // 添加列：列族为 "data"，列名为 "airtemp"，值为温度值
                // Bytes.toBytes() 将 Java 基本类型转换为 HBase 内部使用的字节数组
                p.addColumn(HBaseTemperatureQuery.DATA_COLUMNFAMILY,
                        HBaseTemperatureQuery.AIRTEMP_QUALIFIER,
                        Bytes.toBytes(parser.getAirTemperature()));

                // 将 Put 对象写入 context，供 HFileOutputFormat2 处理
                // ImmutableBytesWritable 包装 rowKey 作为输出 key
                context.write(new ImmutableBytesWritable(rowKey), p);
            }
        }
    }

    /**
     * 作业配置与执行入口<br>
     *<br>
     * 配置步骤：<br>
     * 1. 创建 Job 并设置基本信息（名称、jar类）<br>
     * 2. 配置输入输出路径<br>
     * 3. 设置 Mapper 类及其输入输出类型<br>
     * 4. 使用 HFileOutputFormat2.configureIncrementalLoad() 自动配置分区和排序<br>
     * 5. 执行 job 后使用 LoadIncrementalHFiles 完成批量加载<br>
     *<br>
     * @param args 命令行参数，args[0] 为输入数据路径<br>
     * @return 作业执行状态（0 成功，1 失败）<br>
     * @throws Exception 如果执行过程中发生异常<br>
     */
    @Override
    public int run(String[] args) throws Exception {
        // 参数验证：确保传入输入数据路径
        if (args.length != 1) {
            System.err.println("Usage: HBaseTemperatureBulkImporter <input>");
            return -1;
        }

        // 创建 HBase 配置，合并 Hadoop 和 HBase 配置
        Configuration conf = HBaseConfiguration.create(getConf());

        // 创建 MapReduce 作业
        Job job = Job.getInstance(conf, getClass().getSimpleName());
        job.setJarByClass(getClass());

        // 配置输入路径：从命令行参数指定的目录读取气象数据文件
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 设置临时输出路径，用于存储生成的 HFile
        // 注意：此路径应为 HDFS 上的路径，以便 RegionServer 访问
        Path tmpPath = new Path("/tmp/bulk");
        FileOutputFormat.setOutputPath(job, tmpPath);

        // 设置 Mapper 类
        job.setMapperClass(HBaseTemperatureMapper.class);

        // 指定 Mapper 输出类型：key 为 ImmutableBytesWritable（包装 rowkey），value 为 Put
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        // 创建 Connection、Table、Admin 和 RegionLocator 实例
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("observations"));
        Admin admin = connection.getAdmin();
        RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf("observations"));

        try {
            // 自动配置 incremental load：
            // 1. 根据 table 的 Region 分布设置 partitioner
            // 2. 配置输出格式为 HFileOutputFormat2
            // 3. 确保输出按键（行键）排序，符合 HFile 要求
            // HBase 2.x 版本需要 RegionLocator 参数
            HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

            // 等待 MapReduce 作业完成
            if (!job.waitForCompletion(true)) {
                return 1;
            }

            // 执行批量加载（Bulk Load）：
            // HBase 2.x 版本需要 Admin 和 RegionLocator 参数
            // 此方法将 /tmp/bulk 目录下的 HFile 直接加载到表中，
            // 绕过 WAL 直接写入 StoreFile，性能远高于直接 put
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(tmpPath, admin, table, regionLocator);

            // 清理临时目录
            FileSystem.get(conf).delete(tmpPath, true);

            return 0;
        } finally {
            // 确保 Table 连接关闭，释放资源
            table.close();
            // 关闭 Admin
            admin.close();
            // 关闭 Connection
            connection.close();
        }
    }

    /**
     * 程序入口：使用 ToolRunner 运行此工具<br>
     *<br>
     * ToolRunner 负责：<br>
     * 1. 解析命令行参数<br>
     * 2. 配置 Hadoop/HBase 配置<br>
     * 3. 调用 run() 方法执行作业<br>
     * 4. 返回退出码给操作系统<br>
     *<br>
     * @param args 命令行参数<br>
     * @throws Exception 如果执行过程中发生异常<br>
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new HBaseTemperatureBulkImporter(), args);
        System.exit(exitCode);
    }
}