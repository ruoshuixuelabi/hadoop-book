import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 温度分布统计程序:按温度区间统计记录数量.
 *
 * 本程序演示如何使用Combiner进行局部聚合,优化网络传输:
 *
 * 1. 功能:
 *    - 按温度(取整到十位)分组统计记录数量
 *    - 例如:温度23度归入2区间,温度27度归入2区间
 *    - 输出每个温度区间的记录数
 *
 * 2. 数据流程:
 *    - Map:解析记录,输出(温度区间, 1)
 *    - Combiner:局部聚合,减少shuffle数据量
 *    - Reduce:全局聚合,得到最终统计结果
 *
 * 3. 温度区间说明:
 *    - 温度除以10取整,如:23/10=2, -5/10=0
 *    - 区间2代表20-29度,区间-5代表-59到-50度
 *    - 有符号温度(如 отрицательная)也需要正确处理
 *
 * 4. 输出应用:
 *    - 分析气温分布特征
 *    - 可视化温度直方图
 *    - 极端温度识别
 *
 * @see LongSumReducer
 */
public class TemperatureDistribution extends Configured implements Tool {

    /**
     * 温度计数Mapper:按温度区间输出计数.
     *
     * 将温度按十位取整后作为键,方便统计各温度区间的分布.
     */
    static class TemperatureCountMapper
        extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

        // 使用静态常量避免重复创建对象
        private static final LongWritable ONE = new LongWritable(1);
        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * Map方法:解析并按温度区间统计.
         *
         * @param key     行偏移量
         * @param value   NCDC数据行
         * @param context MapReduce上下文
         * @throws IOException          IO异常
         * @throws InterruptedException 中断异常
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            parser.parse(value);
            if (parser.isValidTemperature()) {
                // 温度除以10取整,得到温度区间
                // 例如:温度23度 -> 区间2, 温度-5度 -> 区间0
                context.write(new IntWritable(parser.getAirTemperature() / 10), ONE);
            }
        }
    }

    /**
     * 运行MapReduce作业,统计温度分布.
     *
     * 使用LongSumReducer进行求和聚合,它同时作为Combiner和Reducer:
     * - Combiner阶段:各Map节点局部聚合,减少网络传输
     * - Reduce阶段:全局聚合,得到最终每个温度区间的记录数
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

        job.setMapperClass(TemperatureCountMapper.class);
        // LongSumReducer既是Combiner也是Reducer
        // Combiner:在map节点局部聚合,减少shuffle数据量
        // Reducer:全局聚合,得到最终结果
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口.
     *
     * @param args 命令行参数:输入路径 输出路径
     * @throws Exception 运行异常
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TemperatureDistribution(), args);
        System.exit(exitCode);
    }
}