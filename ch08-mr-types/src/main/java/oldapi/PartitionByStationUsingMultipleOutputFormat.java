package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.*;

/**
 * 使用 MultipleTextOutputFormat 按气象站ID分区（旧 API）。
 *
 * <p>本示例使用 MultipleTextOutputFormat 根据记录内容动态生成输出文件名。
 */
public class PartitionByStationUsingMultipleOutputFormat extends Configured
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
     * 标准 Reducer。
     */
    static class StationReducer extends MapReduceBase
        implements Reducer<Text, Text, NullWritable, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values,
            OutputCollector<NullWritable, Text> output, Reporter reporter)
            throws IOException {
            while (values.hasNext()) {
                output.collect(NullWritable.get(), values.next());
            }
        }
    }

    /*[*/
    /**
     * 自定义 MultipleTextOutputFormat，根据气象站ID生成文件名。
     */
    static class StationNameMultipleTextOutputFormat
        extends MultipleTextOutputFormat<NullWritable, Text> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * 根据记录内容生成文件名。
         *
         * @param key   键
         * @param value 值（记录内容）
         * @param name  基础名称
         * @return 文件名，即气象站ID
         */
        @Override
        protected String generateFileNameForKeyValue(NullWritable key, Text value,
            String name) {
            parser.parse(value);
            // 返回气象站ID作为文件名
            return parser.getStationId();
        }
    }/*]*/

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
        conf.setReducerClass(StationReducer.class);
        conf.setOutputKeyClass(NullWritable.class);
        // 设置自定义输出格式
        conf.setOutputFormat(StationNameMultipleTextOutputFormat.class);

        // 运行作业
        JobClient.runJob(conf);
        return 0;
    }

    /**
     * 程序入口点。
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(
            new PartitionByStationUsingMultipleOutputFormat(), args);
        System.exit(exitCode);
    }
}
