package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 使用二次排序查找最大温度的程序(旧API版本).
 *
 * 本程序是MaxTemperatureUsingSecondarySort的旧API版本,
 * 使用org.apache.hadoop.mapred包中的旧接口.
 *
 * 二次排序原理:
 * 1. 组合键设计:将年份和温度组合成IntPair
 *    - 第一字段(year):用于分区和分组
 *    - 第二字段(temperature):用于排序
 *
 * 2. 分区策略:FirstPartitioner按年份分区
 *
 * 3. 排序策略:KeyComparator按年份和温度排序(温度降序)
 *
 * 4. 分组策略:GroupComparator只按年份分组
 *    - 同一年份的所有记录在同一个reduce组
 *    - 由于温度已排序,组内第一条是最高温度
 *
 * @see IntPair
 */
public class MaxTemperatureUsingSecondarySort
  extends Configured implements Tool {

    /**
     * 解析NCDC数据并输出(年份,温度)组合键的Mapper.
     */
    static class MaxTemperatureMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, IntPair, NullWritable> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * Map方法:解析记录并输出组合键.
         */
        public void map(LongWritable key, Text value,
            OutputCollector<IntPair, NullWritable> output, Reporter reporter)
            throws IOException {

            parser.parse(value);
            if (parser.isValidTemperature()) {
                // 输出组合键:(年份, 温度),相同年份内按温度降序排列
                output.collect(new IntPair(parser.getYearInt(),
                    + parser.getAirTemperature()), NullWritable.get());
            }
        }
    }

    /**
     * 二次排序Reducer:输出每个年份的最高温度.
     */
    static class MaxTemperatureReducer extends MapReduceBase
        implements Reducer<IntPair, NullWritable, IntPair, NullWritable> {

        /**
         * Reduce方法:输出每个年份的第一条记录(最高温度).
         */
        public void reduce(IntPair key, Iterator<NullWritable> values,
            OutputCollector<IntPair, NullWritable> output, Reporter reporter)
            throws IOException {

            // 输出第一条记录,即该年份的最高温度
            output.collect(key, NullWritable.get());
        }
    }

    /**
     * 分区器:仅按组合键的第一字段(年份)分区.
     */
    public static class FirstPartitioner
        implements Partitioner<IntPair, NullWritable> {

        @Override
        public void configure(JobConf job) {}

        @Override
        public int getPartition(IntPair key, NullWritable value, int numPartitions) {
            // 乘以127增加混合,减少热点
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    /**
     * 键比较器:按年份升序,温度降序排序.
     */
    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(IntPair.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
            if (cmp != 0) {
                return cmp;
            }
            // 温度降序:ip1温度>ip2温度时返回-1,排在前面
            return -IntPair.compare(ip1.getSecond(), ip2.getSecond()); //reverse
        }
    }

    /**
     * 分组比较器:仅按组合键的第一字段(年份)分组.
     */
    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(IntPair.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            return IntPair.compare(ip1.getFirst(), ip2.getFirst());
        }
    }

    /**
     * 运行MapReduce作业,配置二次排序的各种比较器.
     */
    @Override
    public int run(String[] args) throws IOException {
        JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (conf == null) {
            return -1;
        }

        conf.setMapperClass(MaxTemperatureMapper.class);
        // 设置分区器:按年份分区
        conf.setPartitionerClass(FirstPartitioner.class);
        // 设置排序比较器:按年份和温度排序
        conf.setOutputKeyComparatorClass(KeyComparator.class);
        // 设置分组比较器:按年份分组
        conf.setOutputValueGroupingComparator(GroupComparator.class);
        conf.setReducerClass(MaxTemperatureReducer.class);
        conf.setOutputKeyClass(IntPair.class);
        conf.setOutputValueClass(NullWritable.class);

        JobClient.runJob(conf);
        return 0;
    }

    /**
     * 程序入口.
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);
        System.exit(exitCode);
    }
}