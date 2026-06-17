package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

/**
 * 使用 MultipleOutputs 将数据按气象站ID分区到不同文件（旧 API）。
 *
 * <p>本示例使用旧 API 的 MultipleOutputs 实现与新 API 相同的功能：
 * 将数据按气象站ID写入不同的命名文件。
 */
public class PartitionByStationUsingMultipleOutputs extends Configured
  implements Tool {

    /**
     * 解析气象记录的 Mapper。
     */
    static class StationMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * 处理气象记录，提取气象站ID。
         */
        public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

            parser.parse(value);
            output.collect(new Text(parser.getStationId()), value);
        }
    }

    /**
     * 使用 MultipleOutputs 写入多个命名文件的 Reducer。
     */
    static class MultipleOutputsReducer extends MapReduceBase
        implements Reducer<Text, Text, NullWritable, Text> {

        // MultipleOutputs 实例
        private MultipleOutputs multipleOutputs;

        /**
         * 初始化 MultipleOutputs。
         * 在 Task 开始时调用。
         */
        @Override
        public void configure(JobConf conf) {
            multipleOutputs = new MultipleOutputs(conf);
        }

        /**
         * 为每个气象站输出独立的文件。
         */
        @SuppressWarnings("unchecked")
        public void reduce(Text key, Iterator<Text> values,
            OutputCollector<NullWritable, Text> output, Reporter reporter)
            throws IOException {

            // 获取对应气象站的命名输出收集器
            // 并将气象站ID中的 "-" 替换为空（生成干净的文件名）
            OutputCollector collector = multipleOutputs.getCollector("station",
                key.toString().replace("-", ""), reporter);
            while (values.hasNext()) {
                collector.collect(NullWritable.get(), values.next());
            }
        }

        /**
         * 关闭 MultipleOutputs。
         */
        @Override
        public void close() throws IOException {
            multipleOutputs.close();
        }
    }

    /**
     * 运行 MapReduce 作业。
     *
     * @param args [输入路径, 输出路径]
     * @return 作业成功返回 0，失败返回 -1
     */
    @Override
    public int run(String[] args) throws IOException {
        JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (conf == null) {
            return -1;
        }

        // 设置 Mapper 和 Reducer
        conf.setMapperClass(StationMapper.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setReducerClass(MultipleOutputsReducer.class);
        conf.setOutputKeyClass(NullWritable.class);
        // 使用 NullOutputFormat 抑制默认的空 part 文件
        conf.setOutputFormat(NullOutputFormat.class);

        // 注册命名输出 "station"，指定输出格式和键值类型
        MultipleOutputs.addMultiNamedOutput(conf, "station", TextOutputFormat.class,
            NullWritable.class, Text.class);

        // 运行作业
        JobClient.runJob(conf);
        return 0;
    }

    /**
     * 程序入口点。
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PartitionByStationUsingMultipleOutputs(),
            args);
        System.exit(exitCode);
    }
}
