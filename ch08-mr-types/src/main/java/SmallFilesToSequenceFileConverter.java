// cc SmallFilesToSequenceFileConverter 将小文件集合打包为单个 SequenceFile 的 MapReduce 程序

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//vv SmallFilesToSequenceFileConverter
/**
 * 将大量小文件合并为单个 SequenceFile 的示例。
 *
 * <p>HDFS 和 MapReduce 在处理大量小文件时效率很低，因为：
 * <ul>
 *   <li>每个文件都会产生单独的 Map 任务，增加任务调度开销</li>
 *   <li>小文件的块存储效率低，NameNode 内存压力增大</li>
 *   <li>大量随机磁盘 I/O 影响性能</li>
 * </ul>
 *
 * <p>本程序使用 WholeFileInputFormat（将每个文件作为一条记录）
 * 和 SequenceFileOutputFormat（将所有文件合并为一个 SequenceFile）来解决这个问题。
 *
 * <p>SequenceFile 的优势：
 * <ul>
 *   <li>将小文件作为 SequenceFile 的值存储，文件名作为键</li>
 *   <li>大量小文件变成一个 SequenceFile，减少了文件数量</li>
 *   <li>SequenceFile 是 Hadoop 原生格式，支持压缩和分片</li>
 *   <li>后续可以使用 SequenceFileInputFormat 高效处理</li>
 * </ul>
 *
 * <p>用法：
 * <pre>
 * hadoop jar app.jar SmallFilesToSequenceFileConverter input-dir output-file
 * </pre>
 *
 * <p>输出：一个 SequenceFile，包含 (Text 文件名, BytesWritable 文件内容) 对
 */
public class SmallFilesToSequenceFileConverter extends Configured implements Tool {

    /**
     * Mapper，将文件名作为键，文件内容作为值。
     *
     * <p>由于使用了 WholeFileInputFormat，每个 Map 任务处理一个完整文件。
     * 在 setup 阶段获取文件名，后续在 map 阶段输出。
     */
    static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
        // 用于存储文件名（作为输出的键）
        private Text filenameKey;

        /**
         * 初始化，获取输入文件的路径。
         *
         * <p>setup 方法在每个 Map 任务开始时调用一次，
         * 用于初始化 Mapper 所需的数据。
         *
         * @param context Task 上下文
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 获取输入分片信息
            InputSplit split = context.getInputSplit();
            // 转换为 FileSplit 以获取文件路径
            Path path = ((FileSplit) split).getPath();
            // 创建文件名作为输出的键
            filenameKey = new Text(path.toString());
        }

        /**
         * 输出文件名和文件内容。
         *
         * @param key    NullWritable（WholeFileInputFormat 不使用键）
         * @param value  文件内容
         * @param context 输出上下文
         */
        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            // 输出 (文件名, 文件内容) 对
            context.write(filenameKey, value);
        }
    }

    /**
     * 运行 MapReduce 作业。
     *
     * @param args [输入目录, 输出 SequenceFile 路径]
     * @return 作业成功返回 0，失败返回 -1
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }

        // 设置输入格式为 WholeFileInputFormat（每个文件作为一条记录）
        job.setInputFormatClass(WholeFileInputFormat.class);
        // 设置输出格式为 SequenceFileOutputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // 设置输出键类型（文件名）
        job.setOutputKeyClass(Text.class);
        // 设置输出值类型（文件内容）
        job.setOutputValueClass(BytesWritable.class);
        // 设置 Mapper 类
        job.setMapperClass(SequenceFileMapper.class);

        // 提交作业并等待完成
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口点。
     *
     * @param args [输入目录, 输出 SequenceFile 路径]
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
        System.exit(exitCode);
    }
}
// ^^ SmallFilesToSequenceFileConverter
