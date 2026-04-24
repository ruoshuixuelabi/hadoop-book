// cc PartitionByStationUsingMultipleOutputs 使用 MultipleOutputs 将整个数据集按气象站ID分区到不同文件

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

//vv PartitionByStationUsingMultipleOutputs
/**
 * 使用 MultipleOutputs 将数据按气象站ID分区到不同文件的示例。
 *
 * <p>默认情况下，MapReduce 将结果按分区写入 part-r-00000、part-r-00001 等文件。
 * 本示例使用 MultipleOutputs 可以将数据写入以气象站ID命名的文件，
 * 如 station-010010-99999、station-020025-99999 等。
 *
 * <p>MultipleOutputs 的优势：
 * <ul>
 *   <li>可以根据记录内容动态命名输出文件</li>
 *   <li>一个 reduce 可以输出多个命名文件</li>
 *   <li>避免了后期再对输出进行分组处理的麻烦</li>
 * </ul>
 *
 * <p>用法：
 * <pre>
 * hadoop jar app.jar PartitionByStationUsingMultipleOutputs input output
 * </pre>
 *
 * <p>输出文件命名格式：{气象站ID}/part-r-00000
 */
public class PartitionByStationUsingMultipleOutputs extends Configured implements Tool {

    /**
     * 解析气象记录的 Mapper。
     *
     * <p>从每条 NCDC 气象记录中提取气象站ID作为输出的键，
     * 原始记录作为值。
     */
    static class StationMapper extends Mapper<LongWritable, Text, Text, Text> {
        // NCDC 记录解析器
        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * 处理一条气象记录，提取气象站ID。
         *
         * @param key     行偏移量
         * @param value   NCDC 记录行
         * @param context 输出上下文
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 解析记录
            parser.parse(value);
            // 以气象站ID作为键，原记录作为值输出
            context.write(new Text(parser.getStationId()), value);
        }
    }

    /*[*//**
     * 支持 MultipleOutputs 的 Reducer。
     *
     * <p>该 Reducer 不是将所有值写入一个文件，
     * 而是为每个气象站ID创建一个独立的文件。
     */
    static class MultipleOutputsReducer extends Reducer<Text, Text, NullWritable, Text> {
        // MultipleOutputs 实例，用于写入多个命名文件
        private MultipleOutputs<NullWritable, Text> multipleOutputs;

        /**
         * 初始化 MultipleOutputs。
         * setup 方法在每个 Task 开始时调用一次，用于初始化资源。
         *
         * @param context Task 上下文
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 创建 MultipleOutputs 实例
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        }

        /**
         * 为每个气象站输出独立的文件。
         *
         * @param key      气象站ID
         * @param values  该气象站的所有记录
         * @param context 输出上下文
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 遍历所有记录
            for (Text value : values) {
                // 以气象站ID作为文件名写入
                // 这样每个气象站的数据都会写入单独的文件
                multipleOutputs.write(NullWritable.get(), value, key.toString());
            }
        }

        /**
         * 关闭 MultipleOutputs。
         * cleanup 方法在每个 Task 结束时调用，用于释放资源。
         *
         * @param context Task 上下文
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 关闭 MultipleOutputs，确保所有数据写入磁盘
            multipleOutputs.close();
        }
    }/*]*/

    /**
     * 运行 MapReduce 作业。
     *
     * @param args [输入路径, 输出路径]
     * @return 作业成功返回 0，失败返回 -1
     */
    @Override
    public int run(String[] args) throws Exception {
        // 解析输入输出路径
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }

        // 设置 Mapper 类
        job.setMapperClass(StationMapper.class);
        // 设置 Map 输出键类型（气象站ID）
        job.setMapOutputKeyClass(Text.class);
        // 设置 Reducer 类
        job.setReducerClass(MultipleOutputsReducer.class);
        // 设置输出键类型（NullWritable，因为文件名已包含在 MultipleOutputs 中）
        job.setOutputKeyClass(NullWritable.class);

        // 提交作业并等待完成
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口点。
     *
     * @param args [输入路径, 输出路径]
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PartitionByStationUsingMultipleOutputs(), args);
        System.exit(exitCode);
    }
}
//^^ PartitionByStationUsingMultipleOutputs
