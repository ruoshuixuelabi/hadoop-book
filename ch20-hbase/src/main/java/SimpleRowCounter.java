import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 简单的行计数器 - 统计 HBase 表中的行数
 *
 * 使用 MapReduce 对 HBase 表进行全表扫描,通过计数器统计行数。
 *
 * 工作原理:
 * 1. 使用 TableInputFormat 从 HBase 表读取数据
 * 2. Mapper 处理每一行,使用 FirstKeyOnlyFilter 只读取行键
 * 3. 每处理一行,计数器 +1
 * 4. 最终计数器值即为表的行数
 *
 * /*[*
 * FirstKeyOnlyFilter 的作用:
 * - 只返回每行的第一个 KeyValue (行键 + 列族 + 列名)
 * - 不需要读取完整行数据,减少网络传输
 * - 对于仅需计数场景,可以大幅提升性能
 * *]*/
public class SimpleRowCounter extends Configured implements Tool {

  /**
   * Mapper: 统计行数
   *
   * 输入: ImmutableBytesWritable (行键) -> Result (完整行数据)
   * 输出: (行键, Result) -> 无实际输出,只更新计数器
   *
   * 由于使用 NullOutputFormat,没有实际的 reduce 输出
   */
  static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
    // 定义计数器枚举,用于跟踪处理行数
    public static enum Counters { ROWS }

    /**
     * map 方法: 对每行数据计数器 +1
     *
     * @param row 行键 (实际未使用)
     * @param value 完整的行数据 (实际未使用)
     * @param context Mapper 上下文,用于访问计数器
     */
    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) {
      // 每处理一行,计数器 ROWS +1
      context.getCounter(Counters.ROWS).increment(1);
    }
  }

  /**
   * 配置并执行作业
   *
   * 配置要点:
   * 1. 设置要扫描的表名
   * 2. 创建 Scan,设置 FirstKeyOnlyFilter 只读取行键
   * 3. 使用 TableMapReduceUtil.initTableMapperJob() 配置 Mapper
   * 4. 不需要 Reducer,使用 NullOutputFormat
   *
   * @param args 命令行参数, args[0] 为要统计的表名
   * @return 作业执行状态 (0 成功, 1 失败)
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: SimpleRowCounter <tablename>");
      return -1;
    }

    String tableName = args[0];

    // 创建 Scan 对象
    Scan scan = new Scan();
    // 设置过滤器: 只读取每行的第一个 KeyValue (行键部分)
    // 这样可以减少网络传输的数据量,提高扫描效率
    scan.setFilter(new FirstKeyOnlyFilter());

    // 创建 Job
    Job job = new Job(getConf(), getClass().getSimpleName());
    job.setJarByClass(getClass());

    // 配置 Table Mapper Job
    // TableMapReduceUtil.initTableMapperJob(tableName, scan, mapperClass, outputKeyClass, outputValueClass, job)
    //
    // 参数说明:
    // - tableName: 要扫描的表名
    // - scan: 扫描配置
    // - RowCounterMapper.class: Mapper 类
    // - ImmutableBytesWritable.class: 输出 key 类型 (行键)
    // - Result.class: 输出 value 类型 (行数据)
    TableMapReduceUtil.initTableMapperJob(tableName, scan,
        RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);

    // 不需要 Reducer,结果通过计数器输出
    job.setNumReduceTasks(0);

    // 使用 NullOutputFormat,因为没有实际的输出文件
    // 结果通过 job.getCounters() 获取
    job.setOutputFormatClass(NullOutputFormat.class);

    // 等待作业完成
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * main 方法: 运行行计数器
   *
   * 运行方式: hadoop jar xxx.jar SimpleRowCounter tablename
   * 结果: 通过 job 计数器获取行数
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new SimpleRowCounter(), args);
    System.exit(exitCode);
  }
}