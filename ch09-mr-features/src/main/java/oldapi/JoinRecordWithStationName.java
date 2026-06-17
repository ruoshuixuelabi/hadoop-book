package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;

/**
 * Reduce端连接气象记录与站名元数据的程序(旧API版本).
 *
 * 本程序是JoinRecordWithStationName的旧API版本,
 * 使用org.apache.hadoop.mapred包中的旧接口.
 *
 * 连接策略:
 * 1. 使用TextPair作为键:(站点ID, 标记)
 *    - JoinRecordMapper:气象记录标记为"1"
 *    - JoinStationMapper:站点记录标记为"0"
 *
 * 2. 分区和分组:
 *    - KeyPartitioner:按站点ID分区
 *    - FirstComparator:按站点ID分组
 *
 * 3. 连接执行:
 *    - Reduce方法首先获取站点名称
 *    - 然后遍历气象记录与站名连接后输出
 *
 * @see JoinRecordMapper
 * @see JoinStationMapper
 * @see JoinReducer
 */
public class JoinRecordWithStationName extends Configured implements Tool {

    /**
     * 分区器:根据TextPair的第一字段(站点ID)进行分区.
     */
    public static class KeyPartitioner implements Partitioner<TextPair, Text> {
        @Override
        public void configure(JobConf job) {}

        @Override
        public int getPartition(/*[*/TextPair key/*]*/, Text value, int numPartitions) {
            // & Integer.MAX_VALUE确保hashCode为正数
            return (/*[*/key.getFirst().hashCode()/*]*/ & Integer.MAX_VALUE) % numPartitions;
        }
    }

    /**
     * 运行MapReduce作业,配置Reduce端连接.
     *
     * @param args 命令行参数:气象数据输入路径、站点数据输入路径、输出路径
     * @return 作业成功返回0
     * @throws Exception 作业执行异常
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            JobBuilder.printUsage(this, "<ncdc input> <station input> <output>");
            return -1;
        }

        JobConf conf = new JobConf(getConf(), getClass());
        conf.setJobName("Join record with station name");

        Path ncdcInputPath = new Path(args[0]);
        Path stationInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        // 添加气象数据输入
        MultipleInputs.addInputPath(conf, ncdcInputPath,
            TextInputFormat.class, JoinRecordMapper.class);
        // 添加站点元数据输入
        MultipleInputs.addInputPath(conf, stationInputPath,
            TextInputFormat.class, JoinStationMapper.class);
        FileOutputFormat.setOutputPath(conf, outputPath);

        // 设置分区器和分组比较器
        /*[*/conf.setPartitionerClass(KeyPartitioner.class);
        conf.setOutputValueGroupingComparator(TextPair.FirstComparator.class);/*]*/

        conf.setMapOutputKeyClass(TextPair.class);

        conf.setReducerClass(JoinReducer.class);

        conf.setOutputKeyClass(Text.class);

        JobClient.runJob(conf);
        return 0;
    }

    /**
     * 程序入口.
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new JoinRecordWithStationName(), args);
        System.exit(exitCode);
    }
}