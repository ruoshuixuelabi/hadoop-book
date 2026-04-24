// cc MaxTemperatureUsingSecondarySort 应用:通过键排序温度来找最大温度

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv MaxTemperatureUsingSecondarySort

/**
 * 使用二次排序查找最大温度的程序.
 *
 * 本程序演示MapReduce中的二次排序(S Secondary Sort)特性:
 *
 * 1. 组合键设计:将年份和温度组合成IntPair作为键
 *    - 第一字段(year):用于分区和分组
 *    - 第二字段(temperature):用于排序
 *
 * 2. 分区策略:FirstPartitioner按年份分区,确保同一年的记录到同一Reducer
 *
 * 3. 排序策略:KeyComparator按年份和温度排序(温度降序)
 *    - 同一年的记录按温度从高到低排序
 *
 * 4. 分组策略:GroupComparator只按年份分组
 *    - 同一年的所有温度记录会落入同一个reduce组
 *    - 由于温度已排序,组内第一个就是最高温度
 *
 * 这种设计使得每个reduce组的第一条记录就是该年的最高温度,
 * Reducer只需输出第一条记录即可.
 *
 * @see IntPair
 */
public class MaxTemperatureUsingSecondarySort extends Configured implements Tool {

    /**
     * 解析NCDC数据并输出(年份,温度)组合键的Mapper.
     *
     * 将温度作为键的第二字段,使相同年份内温度按降序排列,
     * 这样在reduce阶段可以方便地获取最高温度.
     */
    static class MaxTemperatureMapper extends Mapper<LongWritable, Text, IntPair, NullWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * Map方法:解析记录并输出组合键.
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
                // 输出组合键:(年份, 温度),相同年份内按温度降序排列
                context.write(new IntPair(parser.getYearInt(), parser.getAirTemperature()), NullWritable.get());
            }
        }
    }

    /**
     * 二次排序Reducer:输出每个年份的最高温度.
     *
     * 由于输入已按温度降序排列,每个年份的reduce组中,
     * 第一条记录就是该年的最高温度.
     */
    static class MaxTemperatureReducer extends Reducer<IntPair, NullWritable, IntPair, NullWritable> {

        /**
         * Reduce方法:输出每个年份的第一条记录(最高温度).
         *
         * @param key     (年份, 温度)组合键
         * @param values  相同年份的温度迭代器(已排序)
         * @param context MapReduce上下文
         * @throws IOException          IO异常
         * @throws InterruptedException 中断异常
         */
        @Override
        protected void reduce(IntPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            // 输出第一条记录,即该年份的最高温度
            context.write(key, NullWritable.get());
        }
    }

    /**
     * 分区器:仅按组合键的第一字段(年份)分区.
     *
     * 确保同一年的所有记录发送到同一个Reducer,
     * 这是二次排序能够工作的基础.
     */
    public static class FirstPartitioner extends Partitioner<IntPair, NullWritable> {
        /**
         * 获取分区号:根据年份hash值确定分区.
         *
         * @param key           组合键
         * @param value         值(未使用)
         * @param numPartitions 分区总数
         * @return 分区号
         */
        @Override
        public int getPartition(IntPair key, NullWritable value, int numPartitions) {
            // 乘以127是为了增加一些混合,减少连续年份可能产生的热点
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    /**
     * 键比较器:按年份升序,温度降序排序.
     *
     * 温度使用负比较实现降序排列,使得相同年份内
     * 最高温度排在最前面.
     */
    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(IntPair.class, true);
        }

        /**
         * 比较两个组合键:先比较年份,年份相同则比较温度(降序).
         *
         * @param w1 第一个组合键
         * @param w2 第二个组合键
         * @return 比较结果
         */
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
            if (cmp != 0) {
                return cmp;
            }
            // 温度相同时取较小值(实际是较大温度,因为取反了)
            // 对于排序:如果ip1温度 > ip2温度,返回-1(ip1排在前面)
            return -IntPair.compare(ip1.getSecond(), ip2.getSecond()); //reverse
        }
    }

    /**
     * 分组比较器:仅按组合键的第一字段(年份)分组.
     *
     * 这确保同一个年份的所有记录落入同一个reduce组,
     * 尽管它们在键排序中可能不相邻(因为温度排序).
     */
    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(IntPair.class, true);
        }

        /**
         * 比较两个键的第一字段是否相等:用于决定是否属于同一组.
         *
         * @param w1 第一个组合键
         * @param w2 第二个组合键
         * @return 年份是否相同
         */
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            return IntPair.compare(ip1.getFirst(), ip2.getFirst());
        }
    }

    /**
     * 运行MapReduce作业,配置二次排序的各种比较器.
     *
     * @param args 命令行参数
     * @return 作业成功返回0,否则返回-1
     * @throws Exception 作业执行异常
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }
        job.setMapperClass(MaxTemperatureMapper.class);
        // 设置分区器:按年份分区
        job.setPartitionerClass(FirstPartitioner.class);
        // 设置排序比较器:按年份和温度排序
        job.setSortComparatorClass(KeyComparator.class);
        // 设置分组比较器:按年份分组
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口.
     *
     * @param args 命令行参数
     * @throws Exception 运行异常
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);
        System.exit(exitCode);
    }
}
// ^^ MaxTemperatureUsingSecondarySort