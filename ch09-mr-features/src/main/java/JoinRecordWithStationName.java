// cc JoinRecordWithStationName 应用:将气象记录与站名进行连接

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

// vv JoinRecordWithStationName

/**
 * Reduce端连接气象记录与站名元数据的程序.
 *
 * 本程序演示MapReduce中的Reduce端连接(Reduce-Side Join):
 *
 * 1. 数据来源:
 *    - 气象数据记录文件(NCDC格式):包含站点ID和温度等数据
 *    - 站点元数据文件:包含站点ID和站点名称的映射
 *
 * 2. 连接策略:
 *    - 使用TextPair作为键,包含(站点ID, 标记)两个字段
 *    - JoinRecordMapper:气象记录标记为"1"
 *    - JoinStationMapper:站点记录标记为"0"
 *    - 标记确保站点记录在同站点气象记录之前处理
 *
 * 3. 分区和分组:
 *    - KeyPartitioner:按站点ID分区,相同站点的记录到同一Reducer
 *    - FirstComparator:按站点ID分组,同一站点的所有记录在同一组
 *
 * 4. 连接执行:
 *    - Reduce方法首先获取站点名称(键值对中的第一个值)
 *    - 然后遍历该站点的所有气象记录,与站名连接后输出
 *
 * 注意:此方法需要将所有数据shuffle到Reduce端,效率较低.
 * 对于大文件,可以考虑使用Map端连接(MaxTemperatureByStationNameUsingDistributedCacheFile).
 *
 * @see JoinRecordMapper
 * @see JoinStationMapper
 * @see JoinReducer
 * @see TextPair
 */
public class JoinRecordWithStationName extends Configured implements Tool {

    /**
     * 分区器:根据TextPair的第一字段(站点ID)进行分区.
     *
     * 确保相同站点的所有记录发送到同一个Reducer,
     * 从而实现同一站点数据的聚合和连接.
     */
    public static class KeyPartitioner extends Partitioner<TextPair, Text> {
        /**
         * 获取分区号:基于站点ID的hash值.
         *
         * @param key           TextPair键,第一字段为站点ID
         * @param value         值
         * @param numPartitions 分区总数
         * @return 分区号(0到numPartitions-1)
         */
        @Override
        public int getPartition(/*[*/TextPair key/*]*/, Text value, int numPartitions) {
            // & Integer.MAX_VALUE确保hashCode为正数,避免取模后负数分区
            return (/*[*/key.getFirst().hashCode()/*]*/ & Integer.MAX_VALUE) % numPartitions;
        }
    }

    /**
     * 运行MapReduce作业,配置Reduce端连接.
     *
     * 使用MultipleInputs支持多个不同格式的输入源:
     * - 气象数据:TextInputFormat -> JoinRecordMapper
     * - 站点元数据:TextInputFormat -> JoinStationMapper
     *
     * @param args 命令行参数,需要三个:气象数据输入路径、站点数据输入路径、输出路径
     * @return 作业成功返回0,否则返回-1
     * @throws Exception 作业执行异常
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            JobBuilder.printUsage(this, "<ncdc input> <station input> <output>");
            return -1;
        }

        Job job = new Job(getConf(), "Join weather records with station names");
        job.setJarByClass(getClass());

        Path ncdcInputPath = new Path(args[0]);
        Path stationInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        // 添加气象数据输入:使用JoinRecordMapper进行标记
        MultipleInputs.addInputPath(job, ncdcInputPath,
                TextInputFormat.class, JoinRecordMapper.class);
        // 添加站点元数据输入:使用JoinStationMapper进行标记
        MultipleInputs.addInputPath(job, stationInputPath,
                TextInputFormat.class, JoinStationMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 设置分区器:按站点ID分区,确保同站点数据到同一Reducer
        /*[*/
        job.setPartitionerClass(KeyPartitioner.class);
        // 设置分组比较器:同站点数据分到同一组,便于连接
        job.setGroupingComparatorClass(TextPair.FirstComparator.class);/*]*/

        // 设置Map输出键类型为TextPair,用于传递站点ID和标记
        job.setMapOutputKeyClass(TextPair.class);

        // 设置Reducer处理连接逻辑
        job.setReducerClass(JoinReducer.class);

        // 最终输出键类型为站点名称(Text)
        job.setOutputKeyClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口.
     *
     * @param args 命令行参数:气象数据路径 站点数据路径 输出路径
     * @throws Exception 运行异常
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new JoinRecordWithStationName(), args);
        System.exit(exitCode);
    }
}
// ^^ JoinRecordWithStationName