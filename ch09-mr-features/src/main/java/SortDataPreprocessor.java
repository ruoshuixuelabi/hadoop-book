// cc SortDataPreprocessor MapReduce程序:将气象数据转换为SequenceFile格式

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv SortDataPreprocessor

/**
 * 数据预处理程序:将NCDC文本格式气象数据转换为SequenceFile格式.
 *
 * 本程序是排序演示的预处理步骤:
 *
 * 1. 输入:
 *    - NCDC文本格式气象数据(包含年份、站点ID、温度等)
 *    - 原始文本格式,可读但占用空间大
 *
 * 2. 输出:
 *    - SequenceFile格式(键:温度IntWritable, 值:原始记录Text)
 *    - 使用块压缩和Gzip编码,提高存储效率
 *
 * 3. 处理逻辑:
 *    - Mapper解析每条记录,过滤无效温度
 *    - 有效记录输出(温度, 原始记录)
 *    - 无Reduce阶段(Map-only作业)
 *
 * 4. 用途:
 *    - 为SortByTemperatureUsingHashPartitioner和
 *      SortByTemperatureUsingTotalOrderPartitioner提供输入
 *    - SequenceFile的二进制格式便于后续排序处理
 *
 * 压缩说明:
 * - CompressionType.BLOCK:将多条记录一起压缩,提高压缩率
 * - GzipCodec:平衡压缩率和兼容性
 *
 * @see SequenceFileOutputFormat
 */
public class SortDataPreprocessor extends Configured implements Tool {

    /**
     * 数据清洗Mapper:过滤无效记录并转换为SequenceFile格式.
     *
     * 功能:
     * - 解析NCDC记录
     * - 只输出有效温度的记录
     * - 将温度作为键,原始记录作为值,便于后续排序
     */
    static class CleanerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * Map方法:过滤并输出有效记录.
         *
         * @param key     行偏移量
         * @param value   NCDC数据行
         * @param context MapReduce上下文
         * @throws IOException          IO异常
         * @throws InterruptedException 中断异常
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                // 输出:(温度, 原始记录),温度作为键便于排序
                context.write(new IntWritable(parser.getAirTemperature()), value);
            }
        }
    }

    /**
     * 运行MapReduce作业(仅Mapper阶段).
     *
     * 配置说明:
     * - setNumReduceTasks(0):表示只有Map阶段,直接输出结果
     * - SequenceFileOutputFormat:输出二进制键值对文件
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
        job.setMapperClass(CleanerMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        // 无Reduce阶段,纯Map作业
        job.setNumReduceTasks(0);
        // 使用SequenceFile格式输出,支持二进制压缩
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口.
     *
     * @param args 命令行参数:输入路径 输出路径
     * @throws Exception 运行异常
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortDataPreprocessor(), args);
        System.exit(exitCode);
    }
}
// ^^ SortDataPreprocessor