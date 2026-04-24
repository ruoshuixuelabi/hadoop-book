package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 分析作业计数器,计算缺失温度字段的记录比例(旧API版本).
 *
 * 本程序是MissingTemperatureFields的旧API版本,
 * 使用org.apache.hadoop.mapred包中的旧接口和JobClient API.
 *
 * 功能:
 * 1. 根据作业ID获取JobClient和RunningJob
 * 2. 从作业计数器获取MISSING和MAP_INPUT_RECORDS统计
 * 3. 计算并打印缺失温度字段的记录比例
 *
 * 与新API的区别:
 * - 使用JobClient.getJob获取RunningJob
 * - 使用job.getCounters()获取计数器
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
        // 创建JobClient实例
        JobClient jobClient = new JobClient(new JobConf(getConf()));
        // 根据作业ID获取RunningJob
        RunningJob job = jobClient.getJob(JobID.forName(jobID));
        if (job == null) {
            System.err.printf("No job with ID %s found.\n", jobID);
            return -1;
        }
        // 检查作业是否完成
        if (!job.isComplete()) {
            System.err.printf("Job %s is not complete.\n", jobID);
            return -1;
        }

        // 获取作业的所有计数器
        Counters counters = job.getCounters();
        // 查找自定义计数器:缺失温度记录数
        long missing = counters.getCounter(
            MaxTemperatureWithCounters.Temperature.MISSING);

        // 查找内置计数器:Map处理的输入记录总数
        long total = counters.getCounter(Task.Counter.MAP_INPUT_RECORDS);

        // 计算并打印缺失温度字段的记录比例
        System.out.printf("Records with missing temperature fields: %.2f%%\n",
            100.0 * missing / total);
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