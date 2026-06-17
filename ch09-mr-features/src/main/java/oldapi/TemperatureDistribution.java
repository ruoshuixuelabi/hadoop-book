package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.*;

/**
 * 温度分布统计程序:按温度区间统计记录数量(旧API版本).
 *
 * 本程序是TemperatureDistribution的旧API版本,
 * 使用org.apache.hadoop.mapred包中的旧接口.
 *
 * 功能:
 * - 按温度(取整到十位)分组统计记录数量
 * - 使用LongSumReducer进行聚合
 *
 * @see LongSumReducer
 */
public class TemperatureDistribution extends Configured implements Tool {

    /**
     * 温度计数Mapper:按温度区间输出计数.
     */
    static class TemperatureCountMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, IntWritable, LongWritable> {

        // 使用静态常量避免重复创建对象
        private static final LongWritable ONE = new LongWritable(1);
        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * Map方法:解析并按温度区间统计.
         */
        public void map(LongWritable key, Text value,
            OutputCollector<IntWritable, LongWritable> output, Reporter reporter)
            throws IOException {

            parser.parse(value);
            if (parser.isValidTemperature()) {
                // 温度除以10取整,得到温度区间
                output.collect(new IntWritable(parser.getAirTemperature() / 10), ONE);
            }
        }
    }

    /**
     * 运行MapReduce作业,统计温度分布.
     *
     * @param args 命令行参数,包含输入输出路径
     * @return 作业成功返回0
     * @throws IOException 作业执行异常
     */
    @Override
    public int run(String[] args) throws IOException {
        JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (conf == null) {
            return -1;
        }

        conf.setMapperClass(TemperatureCountMapper.class);
        // LongSumReducer既是Combiner也是Reducer
        conf.setCombinerClass(LongSumReducer.class);
        conf.setReducerClass(LongSumReducer.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(LongWritable.class);

        JobClient.runJob(conf);
        return 0;
    }

    /**
     * 程序入口.
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TemperatureDistribution(), args);
        System.exit(exitCode);
    }
}