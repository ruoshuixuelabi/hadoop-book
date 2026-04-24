// cc MinimalMapReduceWithDefaults 一个最小的 MapReduce 驱动程序，明确设置所有默认值
// 本示例展示了 Hadoop 所有默认配置项，便于理解底层行为

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//vv MinimalMapReduceWithDefaults
/**
 * 展示 MapReduce 作业所有默认配置的示例。
 *
 * <p>与 {@link MinimalMapReduce} 不同，本示例明确设置了所有配置项，
 * 以展示 Hadoop 的默认行为。
 *
 * <p>默认配置说明：
 * <ul>
 *   <li><b>输入格式</b>：TextInputFormat - 按行读取，键为行偏移量（LongWritable），值为行内容（Text）</li>
 *   <li><b>Mapper</b>：IdentityMapper.class - 直接传递输入的键值对，不做任何处理</li>
 *   <li><b>Map 输出键类型</b>：LongWritable - 与输入键类型相同</li>
 *   <li><b>Map 输出值类型</b>：Text - 与输入值类型相同</li>
 *   <li><b>分区器</b>：HashPartitioner - 根据键的哈希值决定分区</li>
 *   <li><b>Reduce 任务数</b>：1 - 只有一个 reduce 任务</li>
 *   <li><b>Reducer</b>：IdentityReducer.class - 直接传递 map 输出的键值对</li>
 *   <li><b>输出键类型</b>：LongWritable - 与 map 输出键类型相同</li>
 *   <li><b>输出值类型</b>：Text - 与 map 输出值类型相同</li>
 *   <li><b>输出格式</b>：TextOutputFormat - 输出为 键\t值 格式</li>
 * </ul>
 *
 * <p>这个示例的目的是教学，帮助理解 Hadoop 的默认行为。
 */
public class MinimalMapReduceWithDefaults extends Configured implements Tool {

    /**
     * 运行 MapReduce 作业，明确设置所有默认配置。
     *
     * @param args 命令行参数
     * @return 作业成功返回 0，失败返回 -1
     * @throws Exception 如果作业执行过程中发生错误
     */
    @Override
    public int run(String[] args) throws Exception {
        // 解析输入和输出路径，如果参数无效则返回 null
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }

        /*[*/
        // 以下是 Hadoop 的默认配置，明确展示以便理解
        // 这些配置在大多数情况下不需要显式设置

        // 设置输入格式为文本输入格式（按行读取）
        job.setInputFormatClass(TextInputFormat.class);
        // 设置 Mapper 类（默认是 IdentityMapper）
        job.setMapperClass(Mapper.class);
        // 设置 Map 输出的键类型（默认 LongWritable）
        job.setMapOutputKeyClass(LongWritable.class);
        // 设置 Map 输出的值类型（默认 Text）
        job.setMapOutputValueClass(Text.class);
        // 设置分区器类（默认 HashPartitioner）
        job.setPartitionerClass(HashPartitioner.class);
        // 设置 reduce 任务数量（默认 1）
        job.setNumReduceTasks(1);
        // 设置 Reducer 类（默认 IdentityReducer）
        job.setReducerClass(Reducer.class);
        // 设置输出的键类型
        job.setOutputKeyClass(LongWritable.class);
        // 设置输出的值类型
        job.setOutputValueClass(Text.class);
        // 设置输出格式（默认 TextOutputFormat）
        job.setOutputFormatClass(TextOutputFormat.class);/*]*/

        // 提交作业并等待完成
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 作业入口点。
     *
     * @param args 命令行参数
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
        System.exit(exitCode);
    }
}
// ^^ MinimalMapReduceWithDefaults
