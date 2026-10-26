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
 * 数一数 HBase 表里有多少行
 *
 * <p>这是一个最小可用的 MapReduce 作业：
 * 用 {@code TableMapper} 扫描表，每读到一行就把计数器加一，
 * 最后用「空输出格式」把结果丢掉，只留下计数器的值
 * <p>用 {@code FirstKeyOnlyFilter} 过滤掉列数据，
 * 这样服务端只返回行键，能省掉大量网络传输
 */
public class SimpleRowCounter extends Configured implements Tool {

  /**
   * 负责数行数的 Mapper
   *
   * <p>{@code TableMapper} 已经把「扫表」这件事封装好了，
   * 这里只需要实现 {@code map} 方法
   */
  static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
    /**
     * 计数器组
     *
     * <p>每个枚举项就是一个独立的计数器，
     * 作业跑完后可以按名字把计数值读出来
     */
    public static enum Counters { ROWS }

    /**
     * 处理一行
     *
     * @param row 行键
     * @param value 这一行的数据
     * @param context 作业上下文，用来访问计数器
     */
    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) {
      // 每读到一行就把 ROWS 计数器加一
      context.getCounter(Counters.ROWS).increment(1);
    }
  }

  /**
   * 作业入口
   *
   * @param args 一个参数：表名
   * @return 0 表示成功，-1 表示参数不对
   * @throws Exception 作业执行失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 必须且只需要一个表名
    if (args.length != 1) {
      System.err.println("Usage: SimpleRowCounter <tablename>");
      // 参数不对，直接返回错误码
      return -1;
    }
    // 要数的表
    String tableName = args[0];
    // 扫描设置
    Scan scan = new Scan();
    // 只要行键，不要列数据
    // 这样能大幅减少从 RegionServer 传回来的字节数
    scan.setFilter(new FirstKeyOnlyFilter());

    // 建立作业，名字用类名
    Job job = new Job(getConf(), getClass().getSimpleName());
    // 告诉作业自己的 jar 在哪儿
    // 提交到集群上时靠这个定位类
    job.setJarByClass(getClass());
    // 用工具类把「扫表 + 用某个 Mapper」这件事一次性配好
    TableMapReduceUtil.initTableMapperJob(tableName, scan,
        RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);
    // 不用 Reduce 阶段：统计数字用计数器就够了
    job.setNumReduceTasks(0);
    // 输出格式设成「什么都不写」
    job.setOutputFormatClass(NullOutputFormat.class);
    // 提交并等待作业跑完
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * 进程入口
   *
   * @param args 一个参数：表名
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会把 HBase 的配置和命令行参数交给这个 Tool
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new SimpleRowCounter(), args);
    // 用退出码告诉外面成功还是失败
    System.exit(exitCode);
  }
}