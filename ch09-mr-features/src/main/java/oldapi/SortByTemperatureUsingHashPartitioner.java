package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 使用HashPartitioner对SequenceFile中的温度数据进行排序(旧API版本).
 *
 * 本程序是SortByTemperatureUsingHashPartitioner的旧API版本,
 * 使用org.apache.hadoop.mapred包中的旧接口.
 *
 * 特点:
 * - 使用默认的HashPartitioner进行分区
 * - 数据均匀分布,但不保证分区内的全局顺序
 * - 输出使用SequenceFile格式,块压缩,Gzip编码
 *
 * @see SequenceFileInputFormat
 * @see SequenceFileOutputFormat
 */
public class SortByTemperatureUsingHashPartitioner extends Configured
  implements Tool {

    /**
     * 运行MapReduce作业,配置基于HashPartitioner的排序.
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
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        // 启用输出压缩:块压缩,Gzip编码
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
        int exitCode = ToolRunner.run(new SortByTemperatureUsingHashPartitioner(),
            args);
        System.exit(exitCode);
    }
}