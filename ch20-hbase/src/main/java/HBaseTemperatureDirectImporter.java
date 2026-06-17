/**
 * 文件：HBaseTemperatureDirectImporter.java<br>
 * 路径：src/main/java/HBaseTemperatureDirectImporter.java<br>
 * 描述：直接使用 Table 将温度数据加载到 HBase 表<br>
 *<br>
 * 功能说明：<br>
 * 直接使用 Table 将气象温度数据导入 HBase 表。<br>
 * 这是三种导入方式中最简单但效率最低的：<br>
 * 1. HBaseTemperatureDirectImporter（本类）：直接 put，自动 flush 开启<br>
 * 2. HBaseTemperatureImporter：通过 TableOutputFormat 批量处理<br>
 * 3. HBaseTemperatureBulkImporter：批量加载 HFile，最高效率<br>
 *<br>
 * 缺点：<br>
 * - 开启 auto-flush，每个 put 后立即刷新到服务器<br>
 * - 网络往返开销大，不适合大规模数据导入<br>
 * - 未利用 MapReduce 的并行处理能力<br>
 *<br>
 * 运行命令：<br>
 * hbase HBaseTemperatureDirectImporter <输入数据路径><br>
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
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
 * 温度数据直接导入器<br>
 * 直接使用 Table 将气象温度数据导入 HBase 表。<br>
 */
public class HBaseTemperatureDirectImporter extends Configured implements Tool {

    /**
     * 温度数据 Mapper 类<br>
     * 在 map 方法中直接使用 Table 执行 put 操作。<br>
     *<br>
     * 关键设计点：<br>
     * - 在 setup() 中创建 Table，整个作业期间复用<br>
     * - map() 中直接调用 table.put() 写入数据<br>
     * - 在 cleanup() 中关闭 Table<br>
     *<br>
     * @param <K> 输入 key 类型<br>
     * @param <V> 输入 value 类型<br>
     */
    static class HBaseTemperatureMapper<K, V> extends Mapper<LongWritable, Text, K, V> {
        // 解析器用于提取气象数据中的站点ID、观测时间，温度
        private NcdcRecordParser parser = new NcdcRecordParser();

        // Table 实例，整个作业期间复用，避免重复创建开销
        private Table table;

        /**
         * setup 阶段：创建 Table 连接<br>
         *<br>
         * setup() 在每个 map task 启动时调用一次，适合执行初始化操作。<br>
         * 此处创建的 Table 会被所有 map() 调用共享，直到 cleanup() 被调用。<br>
         *<br>
         * @param context MapReduce 上下文<br>
         * @throws IOException 如果发生 I/O 错误<br>
         */
        @Override
        protected void setup(Context context) throws IOException {
            // 在 map task 启动时创建 Table 连接
            // HBaseConfiguration.create(context.getConfiguration()) 使用 job 配置创建配置
            // "observations" 是目标表名
            Connection connection = ConnectionFactory.createConnection(
                    HBaseConfiguration.create(context.getConfiguration()));
            this.table = connection.getTable(TableName.valueOf("observations"));
        }

        /**
         * map 方法：解析气象数据并写入 HBase<br>
         *<br>
         * @param key 行偏移量<br>
         * @param value 原始气象数据行<br>
         * @param context MapReduce 上下文<br>
         * @throws IOException 如果发生 I/O 错误<br>
         * @throws InterruptedException 如果处理被中断<br>
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws
                IOException, InterruptedException {
            // 解析原始数据
            parser.parse(value.toString());

            // 只处理有效温度记录
            if (parser.isValidTemperature()) {
                // 生成行键：站点ID(12字节) + 逆序时间戳(8字节)
                byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(),
                        parser.getObservationDate().getTime());

                // 创建 Put 对象
                Put p = new Put(rowKey);

                // 添加列：列族 "data"，列名 "airtemp"，值为温度
                p.addColumn(HBaseTemperatureQuery.DATA_COLUMNFAMILY,
                        HBaseTemperatureQuery.AIRTEMP_QUALIFIER,
                        Bytes.toBytes(parser.getAirTemperature()));

                // 直接写入 HBase
                // 由于 auto-flush 开启，此 put 会立即发送到 RegionServer
                table.put(p);
            }
        }

        /**
         * cleanup 阶段：关闭 Table 连接<br>
         *<br>
         * cleanup() 在 map task 完成时调用，用于释放资源。<br>
         * 确保 Table 被正确关闭，避免连接泄漏。<br>
         *<br>
         * @param context MapReduce 上下文<br>
         * @throws IOException 如果发生 I/O 错误<br>
         */
        @Override
        protected void cleanup(Context context) throws IOException {
            table.close();
        }
    }

    /**
     * 配置并运行 MR 作业<br>
     *<br>
     * 配置要点：<br>
     * - 不使用 Reducer (setNumReduceTasks(0))<br>
     * - 使用 NullOutputFormat，因为输出不是文件而是 HBase 表<br>
     * - Mapper 负责所有数据写入工作<br>
     *<br>
     * @param args 命令行参数，args[0] 为输入路径<br>
     * @return 作业执行状态（0 成功，1 失败）<br>
     * @throws Exception 如果执行过程中发生异常<br>
     */
    @Override
    public int run(String[] args) throws Exception {
        // 参数验证：确保传入输入数据路径
        if (args.length != 1) {
            System.err.println("Usage: HBaseTemperatureDirectImporter <input>");
            return -1;
        }

        // 创建 MapReduce 作业
        // Job.getInstance() 替代废弃的 Job() 构造函数
        Job job = Job.getInstance(getConf(), getClass().getSimpleName());
        job.setJarByClass(getClass());

        // 配置输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 设置 Mapper 类
        job.setMapperClass(HBaseTemperatureMapper.class);

        // 无 Reducer，数据由 Mapper 直接写入 HBase
        job.setNumReduceTasks(0);

        // 使用 NullOutputFormat，因为输出目标是 HBase 表而非文件
        // 此设置避免 MR 尝试写入文件系统
        job.setOutputFormatClass(NullOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口<br>
     *<br>
     * @param args 命令行参数<br>
     * @throws Exception 如果执行过程中发生异常<br>
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new HBaseTemperatureDirectImporter(), args);
        System.exit(exitCode);
    }
}