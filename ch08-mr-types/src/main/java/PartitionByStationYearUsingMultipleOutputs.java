// == PartitionByStationYearUsingMultipleOutputs 按气象站和年份进行二级分区

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 使用 MultipleOutputs 实现二级目录分区的示例。
 *
 * <p>本示例在 {@link PartitionByStationUsingMultipleOutputs} 的基础上，
 * 增加了按年份进行二级分区，创建嵌套的目录结构：
 * <pre>
 * station-010010-99999/2000/part-r-00000
 * station-010010-99999/2001/part-r-00000
 * station-020025-99999/2000/part-r-00000
 * </pre>
 *
 * <p>这种目录结构的特点：
 * <ul>
 *   <li>一级目录：气象站ID</li>
 *   <li>二级目录：年份</li>
 *   <li>文件名：标准 MapReduce 输出文件名</li>
 * </ul>
 *
 * <p>用法：
 * <pre>
 * hadoop jar app.jar PartitionByStationYearUsingMultipleOutputs input output
 * </pre>
 */
public class PartitionByStationYearUsingMultipleOutputs extends Configured implements Tool {

    /**
     * 解析气象记录的 Mapper，提取气象站ID作为键。
     */
    static class StationMapper extends Mapper<LongWritable, Text, Text, Text> {
        // NCDC 记录解析器
        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * 处理气象记录，输出气象站ID和原始记录。
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            // 以气象站ID作为键
            context.write(new Text(parser.getStationId()), value);
        }
    }

    /**
     * 实现二级目录分区的 Reducer。
     */
    static class MultipleOutputsReducer extends Reducer<Text, Text, NullWritable, Text> {
        // MultipleOutputs 实例
        private MultipleOutputs<NullWritable, Text> multipleOutputs;
        // 解析器用于提取年份
        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * 初始化 MultipleOutputs。
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        }

        // vv PartitionByStationYearUsingMultipleOutputs
        /**
         * 按气象站和年份二级目录输出记录。
         *
         * <p>生成的路径格式：{气象站ID}/{年份}/part
         * 例如：station-010010-99999/2000/part
         *
         * @param key    气象站ID
         * @param values 该气象站的所有记录
         * @param context 输出上下文
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                // 解析记录以获取年份
                parser.parse(value);
                // 构建二级目录路径：气象站ID/年份/part
                // basePath 会被 MultipleOutputs 用于创建嵌套目录结构
                String basePath = String.format("%s/%s/part", parser.getStationId(), parser.getYear());
                // 写入 MultipleOutputs，路径中不包含文件名（由框架生成）
                multipleOutputs.write(NullWritable.get(), value, basePath);
            }
        }
        // ^^ PartitionByStationYearUsingMultipleOutputs

        /**
         * 关闭 MultipleOutputs，释放资源。
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
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
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }
        // 设置 Mapper 和 Reducer
        job.setMapperClass(StationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(MultipleOutputsReducer.class);
        job.setOutputKeyClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口点。
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PartitionByStationYearUsingMultipleOutputs(), args);
        System.exit(exitCode);
    }
}
