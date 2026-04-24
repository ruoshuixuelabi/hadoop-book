package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.*;

/**
 * 将小文件集合打包为单个 SequenceFile（旧 API）。
 *
 * <p>使用 WholeFileInputFormat 将每个文件作为一条记录，
 * 然后使用 SequenceFileOutputFormat 输出为单个 SequenceFile。
 */
public class SmallFilesToSequenceFileConverter extends Configured
  implements Tool {

    /**
     * 将文件名作为键、文件内容作为值的 Mapper。
     */
    static class SequenceFileMapper extends MapReduceBase
        implements Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

        // JobConf 配置引用
        private JobConf conf;

        /**
         * 获取输入文件信息。
         */
        @Override
        public void configure(JobConf conf) {
            this.conf = conf;
        }

        /**
         * 输出文件名和文件内容。
         */
        @Override
        public void map(NullWritable key, BytesWritable value,
            OutputCollector<Text, BytesWritable> output, Reporter reporter)
            throws IOException {

            // 从配置中获取输入文件名
            String filename = conf.get("map.input.file");
            output.collect(new Text(filename), value);
        }

    }

    /**
     * 运行 MapReduce 作业。
     *
     * @param args [输入目录, 输出 SequenceFile 路径]
     * @return 作业成功返回 0，失败返回 -1
     */
    @Override
    public int run(String[] args) throws IOException {
        JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (conf == null) {
            return -1;
        }

        // 设置输入格式为 WholeFileInputFormat
        conf.setInputFormat(WholeFileInputFormat.class);
        // 设置输出格式为 SequenceFileOutputFormat
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        // 设置输出键值类型
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(BytesWritable.class);

        // 设置 Mapper 和 Reducer
        conf.setMapperClass(SequenceFileMapper.class);
        conf.setReducerClass(IdentityReducer.class);

        // 运行作业
        JobClient.runJob(conf);
        return 0;
    }

    /**
     * 程序入口点。
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
        System.exit(exitCode);
    }
}
