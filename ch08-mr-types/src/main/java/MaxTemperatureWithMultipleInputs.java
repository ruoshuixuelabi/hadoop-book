// == MaxTemperatureWithMultipleInputs 使用多种输入格式处理气温数据

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

/**
 * 使用 MultipleInputs 处理多种输入格式的气温数据示例。
 *
 * <p>本程序演示如何为不同的输入路径指定不同的输入格式和 Mapper。
 * 它同时处理两种格式的气象数据：
 * <ul>
 *   <li>NCDC 格式数据（使用默认的 MaxTemperatureMapper）</li>
 *   <li>Met Office 格式数据（使用 MetOfficeMaxTemperatureMapper）</li>
 * </ul>
 *
 * <p>MultipleInputs 的优势：
 * <ul>
 *   <li>可以为每个输入路径指定不同的 InputFormat 和 Mapper</li>
 *   <li>不同格式的数据可以用不同的方式解析</li>
 *   <li>代码更清晰，避免在单个 Mapper 中处理多种格式</li>
 * </ul>
 *
 * <p>用法：
 * <pre>
 * hadoop jar app.jar MaxTemperatureWithMultipleInputs ncdc-input metoffice-input output
 * </pre>
 */
public class MaxTemperatureWithMultipleInputs extends Configured implements Tool {

    /**
     * Met Office 气象数据格式的 Mapper。
     *
     * <p>Met Office 格式与 NCDC 格式不同，需要专门的解析器。
     * 该 Mapper 使用 MetOfficeRecordParser 解析数据。
     */
    static class MetOfficeMaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        // Met Office 记录解析器
        private MetOfficeRecordParser parser = new MetOfficeRecordParser();

        /**
         * 处理一条 Met Office 气象记录。
         *
         * @param key     行偏移量
         * @param value   行内容
         * @param context 输出上下文
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 解析记录
            parser.parse(value);
            // 只输出有效的气温记录
            if (parser.isValidTemperature()) {
                // 输出年份作为键，最高气温作为值
                context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
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

        // 创建作业实例，设置作业名称
        // 注意：旧API使用 new Job(getConf(), "name")，新API使用 Job.getInstance()
        Job job = Job.getInstance(getConf(), "Max temperature with multiple input formats");
        // 设置运行作业的主类
        job.setJarByClass(getClass());

        // 解析输入输出路径
        Path ncdcInputPath = new Path(args[0]);
        Path metOfficeInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        // vv MaxTemperatureWithMultipleInputs
        // 使用 MultipleInputs 为不同路径添加不同的输入格式和 Mapper
        // NCDC 数据使用标准 TextInputFormat 和 MaxTemperatureMapper
        MultipleInputs.addInputPath(job, ncdcInputPath, TextInputFormat.class, MaxTemperatureMapper.class);
        // Met Office 数据使用标准 TextInputFormat 和自定义 MetOfficeMaxTemperatureMapper
        MultipleInputs.addInputPath(job, metOfficeInputPath, TextInputFormat.class, MetOfficeMaxTemperatureMapper.class);
        // ^^ MaxTemperatureWithMultipleInputs

        // 设置输出路径
        FileOutputFormat.setOutputPath(job, outputPath);

        // 设置输出键值类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置 Combiner 和 Reducer（都是求最高气温）
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        // 提交作业并等待完成
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口点。
     *
     * @param args [ncdc输入路径, metoffice输入路径, 输出路径]
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureWithMultipleInputs(), args);
        System.exit(exitCode);
    }
}
