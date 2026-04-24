package oldapi;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 使用分布式缓存查找每个站点最高温度并显示站名(旧API版本).
 *
 * 本程序是MaxTemperatureByStationNameUsingDistributedCacheFile的旧API版本,
 * 使用org.apache.hadoop.mapred包中的旧接口和DistributedCache API.
 *
 * Map端连接原理:
 * 1. 将站点元数据文件添加到分布式缓存
 * 2. 在setup阶段加载到内存
 * 3. reduce时通过站点ID查询站名
 *
 * 与Reduce端连接的区别:
 * - 分布式缓存方式:在Map端完成Join,无shuffle
 * - Reduce端方式:需要shuffle所有数据到Reducer
 *
 * 注意:此实现依赖本地文件系统的站点元数据文件,
 * 需要通过 DistributedCache.addCacheFile 或 -files 选项添加.
 *
 * @see NcdcStationMetadata
 */
public class MaxTemperatureByStationNameUsingDistributedCacheFile
  extends Configured implements Tool {

    /**
     * 解析气象数据并输出(站点ID, 温度)对的Mapper.
     */
    static class StationTemperatureMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, IntWritable> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * Map方法:解析气象记录,输出站点ID和温度.
         */
        public void map(LongWritable key, Text value,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

            parser.parse(value);
            if (parser.isValidTemperature()) {
                output.collect(new Text(parser.getStationId()),
                    new IntWritable(parser.getAirTemperature()));
            }
        }
    }

    /**
     * 带站点名称查找的Reducer:查找最大温度并显示站点名称.
     *
     * 在configure方法中初始化站点元数据,
     * 在reduce方法中将站点ID转换为站名输出.
     */
    static class MaxTemperatureReducerWithStationLookup extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, IntWritable> {

        /*[*/private NcdcStationMetadata metadata;/*]*/

        /**
         * configure方法:初始化站点元数据加载器.
         *
         * 在任务开始前调用,加载分布式缓存中的站点元数据文件.
         */
        /*[*/@Override
        public void configure(JobConf conf) {
            metadata = new NcdcStationMetadata();
            try {
                metadata.initialize(new File("stations-fixed-width.txt"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }/*]*/

        /**
         * Reduce方法:查找最大温度并转换为站名输出.
         */
        public void reduce(Text key, Iterator<IntWritable> values,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

            /*[*/String stationName = metadata.getStationName(key.toString());/*]*/

            int maxValue = Integer.MIN_VALUE;
            while (values.hasNext()) {
                maxValue = Math.max(maxValue, values.next().get());
            }
            output.collect(new Text(/*[*/stationName/*]*/), new IntWritable(maxValue));
        }
    }

    /**
     * 运行MapReduce作业,配置Map端连接.
     *
     * @param args 命令行参数,包含输入输出路径
     * @return 作业成功返回0
     * @throws IOException 作业执行异常
     */
    @Override
    public int run(String[] args) throws IOException {
        JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (conf == null) {
            return -1;
        }

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(StationTemperatureMapper.class);
        conf.setCombinerClass(MaxTemperatureReducer.class);
        conf.setReducerClass(MaxTemperatureReducerWithStationLookup.class);

        JobClient.runJob(conf);
        return 0;
    }

    /**
     * 程序入口.
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(
            new MaxTemperatureByStationNameUsingDistributedCacheFile(), args);
        System.exit(exitCode);
    }
}