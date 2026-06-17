// cc MaxTemperatureByStationNameUsingDistributedCacheFile 应用:使用分布式缓存文件查找最大温度并显示站名

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv MaxTemperatureByStationNameUsingDistributedCacheFile

/**
 * 使用分布式缓存查找每个站点最高温度并显示站名的程序.
 *
 * 本程序演示Map端连接(Map-Side Join)的实现方式:
 *
 * 1. 分布式缓存机制:
 *    - 将小表(站点元数据文件)添加到分布式缓存
 *    - Hadoop会自动将文件复制到所有计算节点的相同位置
 *    - 在setup阶段加载到内存,实现高效的Map端连接
 *
 * 2. Map端连接优势:
 *    - 避免了Reduce端的shuffle过程,减少网络传输
 *    - 适合大小表Join场景(大表数据,小表元数据)
 *    - 要求:小表能够完整放入每台机器的内存
 *
 * 3. 与Reduce端连接的区别:
 *    - 分布式缓存方式:在Map端完成Join,无shuffle
 *    - Reduce端方式:需要shuffle所有数据到Reducer
 *    - 本方式性能更好,但只能处理小表
 *
 * 4. 实现细节:
 *    - Mapper输出(站点ID, 温度)
 *    - Reducer的setup方法加载站点元数据到内存
 *    - reduce时通过站点ID查询站名,与温度一起输出
 *
 * 典型用法:首先运行作业生成stations-fixed-width.txt缓存文件
 *
 * @see NcdcStationMetadata
 */
public class MaxTemperatureByStationNameUsingDistributedCacheFile extends Configured implements Tool {

    /**
     * 解析气象数据并输出(站点ID, 温度)对的Mapper.
     *
     * 与标准最大温度程序类似,但输出站点ID而非年份,
     * 以便后续与站点元数据连接.
     */
    static class StationTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        /**
         * Map方法:解析气象记录,输出站点ID和温度.
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
                // 输出站点ID和温度,用于后续聚合求最大值
                context.write(new Text(parser.getStationId()), new IntWritable(parser.getAirTemperature()));
            }
        }
    }

    /**
     * 带站点名称查找的Reducer:查找最大温度并显示站点名称.
     *
     * 在setup阶段从分布式缓存加载站点元数据,
     * 在reduce阶段将站点ID转换为站名输出.
     */
    static class MaxTemperatureReducerWithStationLookup extends Reducer<Text, IntWritable, Text, IntWritable> {
        /*[*/private NcdcStationMetadata metadata;/*]*/

        /**
         * Setup方法:初始化站点元数据加载器.
         *
         * 在每个Reducer任务开始前调用一次,从本地文件系统
         * 加载站点元数据文件到内存,用于后续的站点名称查询.
         *
         * @param context MapReduce上下文
         * @throws IOException          IO异常
         * @throws InterruptedException 中断异常
         */
        /*[*/
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            metadata = new NcdcStationMetadata();
            // 从本地文件系统加载站点元数据
            // 注意:此文件需通过-jobconf或-addCacheFile添加到分布式缓存
            metadata.initialize(new File("stations-fixed-width.txt"));
        }/*]*/

        /**
         * Reduce方法:查找最大温度并转换为站名输出.
         *
         * @param key     站点ID
         * @param values  该站点的所有温度值
         * @param context MapReduce上下文
         * @throws IOException          IO异常
         * @throws InterruptedException 中断异常
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            /*[*/
            // 将站点ID转换为站点名称
            String stationName = metadata.getStationName(key.toString());/*]*/
            int maxValue = Integer.MIN_VALUE;
            // 遍历所有温度值,找出最大值
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            // 输出站点名称和最高温度
            context.write(new Text(/*[*/stationName/*]*/), new IntWritable(maxValue));
        }
    }

    /**
     * 运行MapReduce作业,配置Map端连接.
     *
     * 使用分布式缓存将站点元数据分发到各计算节点,
     * Mapper输出站点数据,Reducer在本地完成站点名称关联.
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
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(StationTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        // 使用带站点名称查找的Reducer
        job.setReducerClass(MaxTemperatureReducerWithStationLookup.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口.
     *
     * @param args 命令行参数:输入路径 输出路径
     * @throws Exception 运行异常
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureByStationNameUsingDistributedCacheFile(), args);
        System.exit(exitCode);
    }
}
// ^^ MaxTemperatureByStationNameUsingDistributedCacheFile