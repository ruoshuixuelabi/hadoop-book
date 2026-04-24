package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;

/**
 * 使用 MultipleInputs 处理多种输入格式的气温数据示例（旧 API）。
 *
 * <p>本程序演示如何为不同的输入路径指定不同的输入格式和 Mapper。
 * 同时处理 NCDC 格式和 Met Office 格式的气象数据。
 *
 * <p>用法：
 * <pre>
 * hadoop jar app.jar oldapi.MaxTemperatureWithMultipleInputs ncdc-input metoffice-input output
 * </pre>
 */
public class MaxTemperatureWithMultipleInputs extends Configured
  implements Tool {

    /**
     * Met Office 气象数据格式的 Mapper。
     */
    static class MetOfficeMaxTemperatureMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, IntWritable> {

        // Met Office 记录解析器
        private MetOfficeRecordParser parser = new MetOfficeRecordParser();

        /**
         * 处理一条 Met Office 气象记录。
         *
         * @param key     行偏移量
         * @param value   行内容
         * @param output  输出收集器
         * @param reporter 进度报告器
         */
        public void map(LongWritable key, Text value,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

            parser.parse(value);
            // 只输出有效的气温记录
            if (parser.isValidTemperature()) {
                output.collect(new Text(parser.getYear()),
                    new IntWritable(parser.getAirTemperature()));
            }
        }
    }

    /**
     * 运行 MapReduce 作业。
     *
     * @param args [ncdc输入路径, metoffice输入路径, 输出路径]
     * @return 作业成功返回 0，失败返回 -1
     */
    @Override
    public int run(String[] args) throws Exception {
        // 检查参数数量
        if (args.length != 3) {
            JobBuilder.printUsage(this, "<ncdc input> <metoffice input> <output>");
            return -1;
        }

        // 创建 JobConf，设置作业名称
        JobConf conf = new JobConf(getConf(), getClass());
        conf.setJobName("Max temperature with multiple input formats");

        // 解析输入输出路径
        Path ncdcInputPath = new Path(args[0]);
        Path metOfficeInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        // 使用 MultipleInputs 添加多个输入路径和对应的 Mapper
        // NCDC 数据使用标准 TextInputFormat 和 MaxTemperatureMapper
        MultipleInputs.addInputPath(conf, ncdcInputPath,
            TextInputFormat.class, MaxTemperatureMapper.class);
        // Met Office 数据使用标准 TextInputFormat 和自定义 MetOfficeMaxTemperatureMapper
        MultipleInputs.addInputPath(conf, metOfficeInputPath,
            TextInputFormat.class, MetOfficeMaxTemperatureMapper.class);

        // 设置输出路径
        FileOutputFormat.setOutputPath(conf, outputPath);

        // 设置输出键值类型
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        // 设置 Combiner 和 Reducer
        conf.setCombinerClass(MaxTemperatureReducer.class);
        conf.setReducerClass(MaxTemperatureReducer.class);

        // 运行作业
        JobClient.runJob(conf);
        return 0;
    }

    /**
     * 程序入口点。
     *
     * @param args [ncdc输入路径, metoffice输入路径, 输出路径]
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureWithMultipleInputs(),
            args);
        System.exit(exitCode);
    }
}
