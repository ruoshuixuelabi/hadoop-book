// cc MaxTemperatureWithCounters 应用:运行最大温度作业,包括统计缺失字段、畸形字段和质量码

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv MaxTemperatureWithCounters

/**
 * 最大温度计算程序,使用计数器统计数据质量问题.
 *
 * 本程序演示如何在MapReduce中使用计数器来:
 * 1. 统计缺失的温度记录数量
 * 2. 统计畸形的温度记录数量
 * 3. 动态统计各质量等级的温度记录数量
 *
 * 通过计数器可以在作业完成后分析输入数据的质量分布情况,
 * 有助于识别数据源中的问题.
 *
 * @see MaxTemperatureReducer
 */
public class MaxTemperatureWithCounters extends Configured implements Tool {

    /**
     * 温度数据质量计数器枚举.
     * 用于区分不同类型的温度记录问题.
     */
    enum Temperature {
        MISSING,    // 温度值缺失
        MALFORMED   // 温度值格式畸形
    }

    /**
     * 带计数器的最大温度Mapper.
     *
     * 功能说明:
     * - 解析NCDC气象数据记录
     * - 识别有效、缺失和畸形三种温度记录
     * - 通过计数器记录各类记录的数量
     * - 使用动态计数器按质量等级分组统计
     */
    static class MaxTemperatureMapperWithCounters extends Mapper<LongWritable, Text, Text, IntWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * Map方法,处理每条NCDC气象记录.
         *
         * @param key     行偏移量(未使用)
         * @param value   NCDC数据行文本
         * @param context MapReduce上下文
         * @throws IOException          IO异常
         * @throws InterruptedException 中断异常
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                // 有效温度记录:输出年份和温度值
                int airTemperature = parser.getAirTemperature();
                context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
            }
            else if (parser.isMalformedTemperature()) {
                // 畸形温度记录:打印错误信息并增加计数器
                System.err.println("Ignoring possibly corrupt input: " + value);
                context.getCounter(Temperature.MALFORMED).increment(1);
            }
            else if (parser.isMissingTemperature()) {
                // 缺失温度记录:增加缺失计数器
                context.getCounter(Temperature.MISSING).increment(1);
            }
            // 动态计数器:按质量等级分组统计,用于分析数据质量分布
            // 质量等级包括:0=有效,1=可疑,9=缺失等
            context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
        }
    }

    /**
     * 运行MapReduce作业.
     *
     * @param args 命令行参数,包含输入输出路径
     * @return 作业成功返回0,否则返回-1
     * @throws Exception 作业执行异常
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(MaxTemperatureMapperWithCounters.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口,使用ToolRunner运行作业.
     *
     * @param args 命令行参数
     * @throws Exception 运行异常
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
        System.exit(exitCode);
    }
}
// ^^ MaxTemperatureWithCounters