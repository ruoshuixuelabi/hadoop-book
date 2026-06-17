/**
 * 文件：SimpleRowCounter.java<br>
 * 路径：src/main/java/SimpleRowCounter.java<br>
 * 描述：简单的行计数器 - 统计 HBase 表中的行数<br>
 *<br>
 * 功能说明：<br>
 * 使用 MapReduce 对 HBase 表进行全表扫描，通过计数器统计行数。<br>
 *<br>
 * 工作原理：<br>
 * 1. 使用 TableInputFormat 从 HBase 表读取数据<br>
 * 2. Mapper 处理每一行，使用 FirstKeyOnlyFilter 只读取行键<br>
 * 3. 每处理一行，计数器 +1<br>
 * 4. 最终计数器值即为表的行数<br>
 *<br>
 * FirstKeyOnlyFilter 的作用：<br>
 * - 只返回每行的第一个 KeyValue（行键 + 列族 + 列名）<br>
 * - 不需要读取完整行数据，减少网络传输<br>
 * - 对于仅需计数场景，可以大幅提升性能<br>
 *<br>
 * HBase 2.x API 变化：<br>
 * - TableMapReduceUtil.initTableMapperJob() 参数保持兼容<br>
 * - 使用 Job.getInstance() 替代废弃的 Job() 构造函数<br>
 *<br>
 * 运行命令：<br>
 * hadoop jar xxx.jar SimpleRowCounter <表名><br>
 */
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 简单的行计数器<br>
 * 统计 HBase 表中的行数。<br>
 */
public class SimpleRowCounter extends Configured implements Tool {

    /**
     * 行计数器 Mapper 类<br>
     *<br>
     * 输入：ImmutableBytesWritable（行键）-> Result（完整行数据）<br>
     * 输出：（行键，Result）-> 无实际输出，只更新计数器<br>
     *<br>
     * 由于没有 Reducer，结果通过计数器输出。<br>
     */
    static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
        // 定义计数器枚举，用于跟踪处理行数
        public static enum Counters { ROWS }

        /**
         * map 方法：对每行数据计数器 +1<br>
         *<br>
         * @param row 行键字节数组（实际未使用）<br>
         * @param value 完整的行数据（实际未使用）<br>
         * @param context Mapper 上下文，用于访问计数器<br>
         * @throws java.io.IOException 如果发生 I/O 错误<br>
         * @throws InterruptedException 如果处理被中断<br>
         */
        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws
                java.io.IOException, InterruptedException {
            // 每处理一行，计数器 ROWS +1
            context.getCounter(Counters.ROWS).increment(1);
        }
    }

    /**
     * 配置并执行作业<br>
     *<br>
     * 配置要点：<br>
     * 1. 设置要扫描的表名<br>
     * 2. 创建 Scan，设置 FirstKeyOnlyFilter 只读取行键<br>
     * 3. 使用 TableMapReduceUtil.initTableMapperJob() 配置 Mapper<br>
     * 4. 不需要 Reducer，使用 TableInputFormat 作为输入格式<br>
     *<br>
     * @param args 命令行参数，args[0] 为要统计的表名<br>
     * @return 作业执行状态（0 成功，1 失败）<br>
     * @throws Exception 如果执行过程中发生异常<br>
     */
    @Override
    public int run(String[] args) throws Exception {
        // 参数验证：确保传入表名
        if (args.length != 1) {
            System.err.println("Usage: SimpleRowCounter <tablename>");
            return -1;
        }

        String tableName = args[0];

        // 创建 Scan 对象
        // Scan 用于扫描 HBase 表，支持设置起止行、过滤器等
        Scan scan = new Scan();

        // 设置过滤器：只读取每行的第一个 KeyValue（行键部分）
        // 这样可以减少网络传输的数据量，提高扫描效率
        // FirstKeyOnlyFilter 只返回每行的第一个 KeyValue
        scan.setFilter(new FirstKeyOnlyFilter());

        // 创建 Job
        // HBase 2.x 推荐使用 Job.getInstance() 替代废弃的 Job() 构造函数
        Job job = Job.getInstance(getConf(), getClass().getSimpleName());
        job.setJarByClass(getClass());

        // 配置 Table Input Format
        // 设置要扫描的表名
        job.getConfiguration().set(TableInputFormat.INPUT_TABLE, tableName);

        // 配置 Table Mapper Job
        // TableMapReduceUtil.initTableMapperJob() 简化了 HBase MapReduce 作业配置
        //
        // 参数说明:
        // - tableName: 要扫描的表名
        // - scan: 扫描配置
        // - RowCounterMapper.class: Mapper 类
        // - ImmutableBytesWritable.class: 输出 key 类型（行键）
        // - Result.class: 输出 value 类型（行数据）
        // - job: MapReduce 作业
        TableMapReduceUtil.initTableMapperJob(tableName, scan,
                RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);

        // 不需要 Reducer，结果通过计数器输出
        job.setNumReduceTasks(0);

        // 等待作业完成
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * main 方法：运行行计数器<br>
     *<br>
     * 运行方式：hadoop jar xxx.jar SimpleRowCounter tablename<br>
     * 结果：通过 job 计数器获取行数<br>
     *<br>
     * @param args 命令行参数<br>
     * @throws Exception 如果执行过程中发生异常<br>
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new SimpleRowCounter(), args);
        System.exit(exitCode);
    }
}