package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 将SequenceFile转换为MapFile格式的程序(旧API版本).
 *
 * 本程序是SortByTemperatureToMapFile的旧API版本,
 * 使用org.apache.hadoop.mapred包中的旧接口.
 *
 * MapFile特点:
 * - SequenceFile的索引版本,支持按键快速查找
 * - 索引按建排序,每N条记录建一个索引
 * - 支持二分查找,定位效率高于SequenceFile
 *
 * @see MapFileOutputFormat
 */
public class SortByTemperatureToMapFile extends Configured implements Tool {

    /**
     * 运行MapReduce作业,将SequenceFile转换为MapFile.
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

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputKeyClass(IntWritable.class);
        // 使用MapFileOutputFormat输出,自动创建索引
        /*[*/conf.setOutputFormat(MapFileOutputFormat.class);/*]*/
        SequenceFileOutputFormat.setCompressOutput(conf, true);
        SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(conf,
            CompressionType.BLOCK);

        JobClient.runJob(conf);
        return 0;
    }

    /**
     * 程序入口.
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortByTemperatureToMapFile(), args);
        System.exit(exitCode);
    }
}