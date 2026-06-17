package oldapi;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 使用DistributedCache API查找每个站点最高温度并显示站名(旧API版本).
 *
 * 本程序是MaxTemperatureByStationNameUsingDistributedCacheFile的补充版本,
 * 展示旧API中DistributedCache的典型用法.
 *
 * 与MaxTemperatureByStationNameUsingDistributedCacheFile的区别:
 * - 本版本使用DistributedCache.getLocalCacheFiles获取缓存文件
 * - 在configure方法中明确处理本地化文件路径
 *
 * 典型用法:
 * 1. 将站点元数据文件通过-addCacheFile添加到分布式缓存
 * 2. 作业运行时,文件会被复制到本地文件系统
 * 3. 使用DistributedCache.getLocalCacheFiles获取本地路径
 *
 * @see DistributedCache
 */
public class MaxTemperatureByStationNameUsingDistributedCacheFileApi
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
     * 带站点名称查找的Reducer:使用DistributedCache加载站点元数据.
     *
     * 通过DistributedCache.getLocalCacheFiles获取缓存文件列表,
     * 并加载第一个文件作为站点元数据源.
     */
    static class MaxTemperatureReducerWithStationLookup extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, IntWritable> {

        private NcdcStationMetadata metadata;

        /**
         * configure方法:通过DistributedCache获取站点元数据文件.
         *
         * @param conf JobConf配置对象
         */
        @Override
        public void configure(JobConf conf) {
            metadata = new NcdcStationMetadata();
            try {
                // 获取分布式缓存中的本地文件路径列表
                Path[] localPaths = DistributedCache.getLocalCacheFiles(conf);
                if (localPaths.length == 0) {
                    throw new FileNotFoundException("Distributed cache file not found.");
                }
                // 使用第一个缓存文件(站点元数据)
                File localFile = new File(localPaths[0].toString());
                metadata.initialize(localFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Reduce方法:查找最大温度并转换为站名输出.
         */
        public void reduce(Text key, Iterator<IntWritable> values,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

            // 将站点ID转换为站点名称
            String stationName = metadata.getStationName(key.toString());

            int maxValue = Integer.MIN_VALUE;
            while (values.hasNext()) {
                maxValue = Math.max(maxValue, values.next().get());
            }
            // 输出站点名称和最高温度
            output.collect(new Text(stationName), new IntWritable(maxValue));
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
            new MaxTemperatureByStationNameUsingDistributedCacheFileApi(), args);
        System.exit(exitCode);
    }
}