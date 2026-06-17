/**
 * 文件：HBaseTemperatureImporter.java<br>
 * 路径：src/main/java/HBaseTemperatureImporter.java<br>
 * 描述：使用 HBase 的 TableOutputFormat 将温度数据导入 HBase 表<br>
 *<br>
 * 功能说明：<br>
 * 使用 HBase 的 MapReduce 输出格式 TableOutputFormat，<br>
 * 将气象观测数据（温度）批量导入 HBase 的 observations 表。<br>
 *<br>
 * 数据模型：<br>
 * - 表名：observations<br>
 * - 行键：站点ID + 观测时间的时间戳<br>
 * - 列族：info<br>
 * - 列：info:airtemp（气温）<br>
 *<br>
 * 算法流程：<br>
 * 1. Map 阶段：读取气象数据文件，解析记录<br>
 * 2. 过滤无效温度数据<br>
 * 3. 构建行键和 Put 对象<br>
 * 4. 通过 TableOutputFormat 直接写入 HBase 表<br>
 *<br>
 * 运行命令：<br>
 * hbase HBaseTemperatureImporter <气象数据文件路径><br>
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
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
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
 * 温度数据导入器<br>
 * 使用 MapReduce 和 TableOutputFormat 将气象温度数据导入 HBase。<br>
 */
public class HBaseTemperatureImporter extends Configured implements Tool {

    /**
     * 温度数据 Mapper 类<br>
     * 读取气象数据文件，解析记录，并将有效数据写入 HBase 表。<br>
     *<br>
     * 输入：LongWritable（行偏移）-> Text（原始气象数据行）<br>
     * 输出：LongWritable（行偏移）-> Put（HBase 写入操作）<br>
     *<br>
     * 注意：此类作为 Mapper 直接写入 HBase 表，不输出到文件系统。<br>
     */
    static class HBaseTemperatureMapper extends Mapper<LongWritable, Text, LongWritable, Put> {
        // 气象记录解析器
        // NcdcRecordParser 用于解析 NOAA 气象数据格式
        private NcdcRecordParser parser = new NcdcRecordParser();

        // Table 实例
        // 在 setup 阶段创建，整个 map 任务期间复用
        private Table table;

        /**
         * Mapper 初始化方法<br>
         * 在每个 map 任务开始前调用一次，用于初始化资源。<br>
         *<br>
         * @param context MapReduce 上下文<br>
         * @throws IOException 如果发生 I/O 错误<br>
         */
        @Override
        protected void setup(Context context) throws IOException {
            // 在任务开始前创建 HBase 表客户端
            // 避免每个记录都创建新的连接，提高性能
            // 使用 HBase 2.x Connection API
            Connection connection = ConnectionFactory.createConnection(
                    HBaseConfiguration.create(context.getConfiguration()));
            this.table = connection.getTable(TableName.valueOf("observations"));
        }

        /**
         * 地图处理方法<br>
         * 解析每行气象数据，过滤无效记录，并写入 HBase。<br>
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
            // 解析气象数据记录
            // parser.parse() 会提取站点ID、观测时间、气温等字段
            parser.parse(value.toString());

            // 检查温度是否有效
            // isValidTemperature() 会过滤掉缺失值（9999）等无效数据
            if (parser.isValidTemperature()) {
                // 构建行键：站点ID + 观测时间的时间戳
                // RowKeyConverter.makeObservationRowKey() 生成唯一的行键
                byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(),
                        parser.getObservationDate().getTime());

                // 创建 Put 对象
                // Put 表示一次 HBase 写入操作
                Put p = new Put(rowKey);

                // 添加气温列：列族 data，列名 airtemp
                // Bytes.toBytes() 将 Java 类型转换为字节数组
                p.addColumn(HBaseTemperatureQuery.DATA_COLUMNFAMILY,
                        HBaseTemperatureQuery.AIRTEMP_QUALIFIER,
                        Bytes.toBytes(parser.getAirTemperature()));

                // 执行写入操作
                // 直接写入 HBase 表（通过 TableOutputFormat 配置）
                table.put(p);
            }
        }

        /**
         * Mapper 清理方法<br>
         * 在每个 map 任务结束后调用，用于释放资源。<br>
         *<br>
         * @param context MapReduce 上下文<br>
         * @throws IOException 如果发生 I/O 错误<br>
         */
        @Override
        protected void cleanup(Context context) throws IOException {
            // 关闭 HBase 表连接，释放资源
            table.close();
        }
    }

    /**
     * 运行 MapReduce 作业<br>
     *<br>
     * @param args 命令行参数，args[0] 为输入数据文件路径<br>
     * @return 执行状态（0 成功，1 失败）<br>
     * @throws Exception 如果执行过程中发生异常<br>
     */
    @Override
    public int run(String[] args) throws Exception {
        // 参数验证：确保传入输入文件路径
        if (args.length != 1) {
            System.err.println("Usage: HBaseTemperatureImporter <input>");
            return -1;
        }

        // 创建 MapReduce 作业
        // Job.getInstance() 替代废弃的 Job() 构造函数
        Job job = Job.getInstance(getConf(), getClass().getSimpleName());

        // 设置作业 JAR 类
        // setJarByClass() 确保正确打包和分发作业
        job.setJarByClass(getClass());

        // 添加输入路径
        // FileInputFormat.addInputPath() 设置 MapReduce 的输入数据路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 配置 TableOutputFormat
        // 设置输出目标表名为 "observations"
        // TableOutputFormat 是 HBase 提供的 MapReduce 输出格式
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "observations");

        // 设置 Mapper 类
        // 指定使用 HBaseTemperatureMapper 处理数据
        job.setMapperClass(HBaseTemperatureMapper.class);

        // 设置 Reduce 任务数量为 0
        // 数据直接写入 HBase，不需要 Reduce 阶段
        job.setNumReduceTasks(0);

        // 设置输出格式
        // TableOutputFormat 用于直接写入 HBase 表
        job.setOutputFormatClass(TableOutputFormat.class);

        // 提交作业并等待完成
        // waitForCompletion() 会阻塞直到作业完成
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口<br>
     * 使用 ToolRunner 运行导入工具。<br>
     *<br>
     * @param args 命令行参数<br>
     * @throws Exception 如果执行过程中发生异常<br>
     */
    public static void main(String[] args) throws Exception {
        // 使用 ToolRunner 运行导入工具
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new HBaseTemperatureImporter(), args);

        System.exit(exitCode);
    }
}