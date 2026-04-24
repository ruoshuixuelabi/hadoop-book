package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

/**
 * 展示 MapReduce 所有默认配置的示例（旧 API）。
 *
 * <p>与 {@link MinimalMapReduce} 不同，本示例明确设置了所有配置项，
 * 以展示 Hadoop 旧 API 的默认行为。
 *
 * <p>默认配置说明：
 * <ul>
 *   <li><b>输入格式</b>：TextInputFormat - 按行读取</li>
 *   <li><b>Map 任务数</b>：由 InputFormat 的分片数决定</li>
 *   <li><b>Mapper</b>：IdentityMapper - 直接传递键值对</li>
 *   <li><b>MapRunner</b>：MapRunner - 默认的 Map 任务运行器</li>
 *   <li><b>Map 输出键类型</b>：LongWritable</li>
 *   <li><b>Map 输出值类型</b>：Text</li>
 *   <li><b>分区器</b>：HashPartitioner</li>
 *   <li><b>Reduce 任务数</b>：1</li>
 *   <li><b>Reducer</b>：IdentityReducer</li>
 *   <li><b>输出键类型</b>：LongWritable</li>
 *   <li><b>输出值类型</b>：Text</li>
 *   <li><b>输出格式</b>：TextOutputFormat</li>
 * </ul>
 */
public class MinimalMapReduceWithDefaults extends Configured implements Tool {

    /**
     * 运行 MapReduce 作业，明确设置所有默认配置。
     *
     * @param args 命令行参数
     * @return 作业成功返回 0，失败返回 -1
     * @throws IOException 如果发生 I/O 错误
     */
    @Override
    public int run(String[] args) throws IOException {
        // 解析输入输出路径
        JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (conf == null) {
            return -1;
        }

        /*[*/
        // 以下是 Hadoop 的默认配置，明确展示以便理解

        // 设置输入格式
        conf.setInputFormat(TextInputFormat.class);
        // 设置 Map 任务数（这里设为 1 仅作为示例，默认由分片数决定）
        conf.setNumMapTasks(1);
        // 设置 Mapper 类
        conf.setMapperClass(IdentityMapper.class);
        // 设置 MapRunner 类
        conf.setMapRunnerClass(MapRunner.class);
        // 设置 Map 输出键类型
        conf.setMapOutputKeyClass(LongWritable.class);
        // 设置 Map 输出值类型
        conf.setMapOutputValueClass(Text.class);
        // 设置分区器类
        conf.setPartitionerClass(HashPartitioner.class);
        // 设置 Reduce 任务数
        conf.setNumReduceTasks(1);
        // 设置 Reducer 类
        conf.setReducerClass(IdentityReducer.class);
        // 设置输出键类型
        conf.setOutputKeyClass(LongWritable.class);
        // 设置输出值类型
        conf.setOutputValueClass(Text.class);
        // 设置输出格式
        conf.setOutputFormat(TextOutputFormat.class);/*]*/

        // 运行作业
        JobClient.runJob(conf);
        return 0;
    }

    /**
     * 程序入口点。
     *
     * @param args 命令行参数
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
        System.exit(exitCode);
    }
}
