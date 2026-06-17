package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 最大温度计算程序,使用计数器统计数据质量问题(旧API版本).
 *
 * 本程序是MaxTemperatureWithCounters的旧API版本,
 * 使用org.apache.hadoop.mapred包中的旧接口.
 *
 * 功能说明:
 * 1. 解析NCDC气象数据记录
 * 2. 使用枚举计数器统计:
 *    - MISSING:缺失的温度记录
 *    - MALFORMED:畸形的温度记录
 * 3. 使用动态计数器按质量等级分组统计
 *
 * 与新API的区别:
 * - 使用MapReduceBase实现Mapper
 * - 使用OutputCollector收集输出
 * - 使用Reporter.incrCounter增加计数器
 *
 * @see oldapi.MaxTemperatureReducer
 */
public class MaxTemperatureWithCounters extends Configured implements Tool {

    /**
     * 温度数据质量计数器枚举.
     */
    enum Temperature {
        MISSING,    // 温度值缺失
        MALFORMED   // 温度值格式畸形
    }

    /**
     * 带计数器的最大温度Mapper(旧API版本).
     *
     * 使用旧API的MapReduceBase和OutputCollector接口.
     */
    static class MaxTemperatureMapperWithCounters extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, IntWritable> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * Map方法:处理NCDC气象记录,使用计数器统计问题.
         *
         * @param key        行偏移量
         * @param value      NCDC数据行
         * @param output     输出收集器
         * @param reporter   作业Reporter,用于更新计数器
         * @throws IOException IO异常
         */
        public void map(LongWritable key, Text value,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

            parser.parse(value);
            if (parser.isValidTemperature()) {
                // 有效温度记录:输出年份和温度值
                int airTemperature = parser.getAirTemperature();
                output.collect(new Text(parser.getYear()),
                    new IntWritable(airTemperature));
            } else if (parser.isMalformedTemperature()) {
                // 畸形温度记录:打印错误信息并更新计数器
                System.err.println("Ignoring possibly corrupt input: " + value);
                reporter.incrCounter(Temperature.MALFORMED, 1);
            } else if (parser.isMissingTemperature()) {
                // 缺失温度记录:更新缺失计数器
                reporter.incrCounter(Temperature.MISSING, 1);
            }

            // 动态计数器:按质量等级分组统计
            // 质量等级包括:0=有效,1=可疑,9=缺失等
            reporter.incrCounter("TemperatureQuality", parser.getQuality(), 1);

        }
    }

    /**
     * 运行MapReduce作业.
     *
     * @param args 命令行参数,包含输入输出路径
     * @return 作业成功返回0,否则返回-1
     * @throws IOException 作业执行异常
     */
    @Override
    public int run(String[] args) throws IOException {
        JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (conf == null) {
            return -1;
        }

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(MaxTemperatureMapperWithCounters.class);
        conf.setCombinerClass(MaxTemperatureReducer.class);
        conf.setReducerClass(MaxTemperatureReducer.class);

        JobClient.runJob(conf);
        return 0;
    }

    /**
     * 程序入口.
     *
     * @param args 命令行参数
     * @throws Exception 运行异常
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
        System.exit(exitCode);
    }
}