// cc MissingTemperatureFields 应用:计算缺失温度字段的记录比例

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

/**
 * 分析作业计数器,计算缺失温度字段的记录占比.
 *
 * 本程序演示如何读取已完成MapReduce作业的计数器信息:
 *
 * 1. 输入:
 *    - 作业ID(如job_1234567890123_0001)
 *    - 通过JobClient获取作业信息和计数器
 *
 * 2. 计数器类型:
 *    - 自定义计数器:Temperature.MISSING (MaxTemperatureWithCounters定义)
 *    - 内置计数器:TaskCounter.MAP_INPUT_RECORDS (Map处理的记录总数)
 *
 * 3. 计算逻辑:
 *    - 获取MISSING计数器值:缺失温度的记录数
 *    - 获取MAP_INPUT_RECORDS:总记录数
 *    - 计算比例:缺失数/总数 * 100%
 *
 * 4. 使用场景:
 *    - 数据质量分析
 *    - 作业运行后的后处理分析
 *    - 验证数据预处理效果
 *
 * 典型用法:
 * 先运行MaxTemperatureWithCounters生成作业,
 * 再运行本程序分析作业ID的计数器数据.
 *
 * @see MaxTemperatureWithCounters
 */
public class MissingTemperatureFields extends Configured implements Tool {

    /**
     * 运行分析程序,读取作业计数器并计算缺失比例.
     *
     * @param args 命令行参数,第一个参数是作业ID
     * @return 成功返回0,参数错误或作业不存在返回-1
     * @throws Exception 执行异常
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            JobBuilder.printUsage(this, "<job ID>");
            return -1;
        }
        String jobID = args[0];
        // 创建Cluster实例,通过配置连接集群
        Cluster cluster = new Cluster(getConf());
        // 根据作业ID获取Job实例
        Job job = cluster.getJob(JobID.forName(jobID));
        if (job == null) {
            System.err.printf("No job with ID %s found.\n", jobID);
            return -1;
        }
        // 检查作业是否完成,未完成则无法获取计数器
        if (!job.isComplete()) {
            System.err.printf("Job %s is not complete.\n", jobID);
            return -1;
        }
        // 获取作业的所有计数器
        Counters counters = job.getCounters();
        // 查找自定义计数器:缺失温度记录数
        long missing = counters.findCounter(MaxTemperatureWithCounters.Temperature.MISSING).getValue();
        // 查找内置计数器:Map处理的输入记录总数
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        // 计算并打印缺失温度字段的记录比例
        System.out.printf("Records with missing temperature fields: %.2f%%\n", 100.0 * missing / total);
        return 0;
    }

    /**
     * 程序入口.
     *
     * @param args 命令行参数:作业ID
     * @throws Exception 运行异常
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MissingTemperatureFields(), args);
        System.exit(exitCode);
    }
}