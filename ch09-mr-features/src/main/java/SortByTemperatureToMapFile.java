import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 将SequenceFile转换为MapFile格式的程序.
 *
 * 本程序演示MapFile的创建和使用:
 *
 * 1. MapFile特点:
 *    - SequenceFile的索引版本,支持按键快速查找
 *    - 索引按建排序,每N条记录建一个索引(默认128条)
 *    - 支持二分查找,定位效率高于SequenceFile
 *
 * 2. 与SequenceFile的区别:
 *    - SequenceFile:只能顺序遍历,查找需要全表扫描
 *    - MapFile:有索引,可按键快速定位
 *    - MapFile占用空间略大(索引开销),但查找更快
 *
 * 3. 输出结构:
 *    - data/: 序列化的数据文件
 *    - index/: 索引文件,存储键和文件偏移量映射
 *
 * 4. 使用场景:
 *    - 需要按键随机访问的大数据文件
 *    - 查找操作频繁的场景
 *    - 作为MapReduce的输入格式(LookupRecordByTemperature)
 *
 * 压缩说明:
 * - 使用BLOCK压缩提高压缩效率
 * - Gzip编码平衡压缩率和兼容性
 *
 * @see MapFileOutputFormat
 * @see SequenceFileInputFormat
 */
public class SortByTemperatureToMapFile extends Configured implements Tool {

    /**
     * 运行MapReduce作业,将SequenceFile转换为MapFile.
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

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        // 使用MapFileOutputFormat输出,自动创建索引
        job.setOutputFormatClass(MapFileOutputFormat.class);
        // 配置输出压缩:BLOCK压缩,Gzip编码
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job,
            CompressionType.BLOCK);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口.
     *
     * @param args 命令行参数:输入路径 输出路径
     * @throws Exception 运行异常
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortByTemperatureToMapFile(), args);
        System.exit(exitCode);
    }
}