package v4;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import v1.MaxTemperatureReducer;

/**
 * 最高温度MapReduce作业驱动器（版本4）
 * <p>
 * 此版本是最终版本，集成了v4版本的增强功能。
 * <p>
 * 主要特点：<br/>
 * - 使用v4版本Mapper，支持检测格式错误的温度记录<br/>
 * - Combiner用于本地聚合，减少网络传输数据量<br/>
 * - Reducer计算每年的全局最高温度
 * <p>
 * Tool接口优势：<br/>
 * - 支持GenericOptions（-conf, -D等标准选项）<br/>
 * - 统一的作业执行方式<br/>
 * - 便于与Hadoop工具链集成
 * <p>
 * 作业配置说明：<br/>
 * - 输入：NCDC气象数据文件/目录<br/>
 * - 输出：每年最高温度的键值对文件<br/>
 * - Mapper输出：(年份, 温度)<br/>
 * - Reducer输出：(年份, 最高温度)
 *
 * @see MaxTemperatureMapper
 * @see MaxTemperatureReducer
 * @see org.apache.hadoop.util.Tool
 */
//Identical to v3 except for v4 mapper
public class MaxTemperatureDriver extends Configured implements Tool {
    /**
     * 执行MapReduce作业
     * <p>
     * 配置流程：<br/>
     * 1. 参数校验<br/>
     * 2. 创建Job实例并设置名称<br/>
     * 3. 设置输入输出路径<br/>
     * 4. 设置Mapper、Combiner、Reducer<br/>
     * 5. 设置输出类型<br/>
     * 6. 提交作业
     *
     * @param args 命令行参数，[input, output]
     * @return 作业成功返回0，失败返回1
     * @throws Exception 作业执行异常
     */
    @Override
    public int run(String[] args) throws Exception {
        // 校验参数数量
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        // 创建作业并设置名称
        Job job = new Job(getConf(), "Max temperature");
        // 设置驱动类用于确定JAR包
        job.setJarByClass(getClass());
        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 设置Mapper、Combiner（本地聚合）、Reducer（全局聚合）
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        // 设置输出键值类型（必须与Mapper输出一致）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 提交作业并等待完成，返回成功/失败状态
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口方法
     * <p>
     * 使用ToolRunner执行，支持Hadoop标准选项
     *
     * @param args 命令行参数
     * @throws Exception 异常
     */
    public static void main(String[] args) throws Exception {
        // 使用ToolRunner执行，支持Hadoop标准选项
        int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
        System.exit(exitCode);
    }
}