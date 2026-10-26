// cc MissingTemperatureFields Application to calculate the proportion of records with missing temperature fields
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

/**
 * 统计某个作业里温度字段缺测的记录比例
 *
 * <p>计数器属于「作业」而不是某段代码，
 * 作业跑完之后仍然可以从作业历史里读回来
 * <p>本例正是这样做的：按作业 ID 取回已经完成的作业，
 * 用它的计数器算出缺测比例
 */
public class MissingTemperatureFields extends Configured implements Tool {

  /**
   * 查询作业计数器并计算缺测比例
   *
   * @param args 唯一参数是作业 ID
   * @return 0 表示查询成功，-1 表示参数错误或作业不存在
   * @throws Exception 查询失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 只需要一个参数：作业 ID
    if (args.length != 1) {
      // 打印用法提示
      JobBuilder.printUsage(this, "<job ID>");
      // 参数错误，返回 -1
      return -1;
    }
    // 要查询的作业 ID
    String jobID = args[0];
    // 连上集群，准备查历史作业
    Cluster cluster = new Cluster(getConf());
    // 按 ID 取回那个作业
    Job job = cluster.getJob(JobID.forName(jobID));
    // 没找到这个作业
    if (job == null) {
      // 报告作业不存在
      System.err.printf("No job with ID %s found.\n", jobID);
      // 找不到作业，返回 -1
      return -1;
    }
    // 作业还没跑完，计数器是不完整的
    if (!job.isComplete()) {
      // 提示作业尚未结束
      System.err.printf("Job %s is not complete.\n", jobID);
      // 作业未完成，返回 -1
      return -1;
    }

    // 取作业的计数器
    Counters counters = job.getCounters();
    // 温度缺测的记录数
    long missing = counters.findCounter(
        MaxTemperatureWithCounters.Temperature.MISSING).getValue();
    // Map 读入的总记录数
    long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();

    // 算出缺测比例并打印
    System.out.printf("Records with missing temperature fields: %.2f%%\n",
        100.0 * missing / total);
    // 查询成功
    return 0;
  }
  /**
   * 程序入口
   *
   * @param args 命令行参数
   * @throws Exception 查询失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会解析通用选项并调用 run
    int exitCode = ToolRunner.run(new MissingTemperatureFields(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
