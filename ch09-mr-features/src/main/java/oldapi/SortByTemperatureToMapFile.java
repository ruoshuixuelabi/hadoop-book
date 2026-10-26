package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 把按气温排序的结果输出成 MapFile（v1 版）
 *
 * <p>MapFile 在有序的 SequenceFile 之上又加了一份索引，
 * 因此既可以像普通文件那样顺序遍历，也可以按键随机查找
 * <p>本例只输出不排序：排序由上游作业完成，
 * 这里负责把结果转成便于随机查找的格式
 */
public class SortByTemperatureToMapFile extends Configured implements Tool {
  
  /**
   * 运行作业
   *
   * @param args 依次为输入路径与输出路径
   * @return 0 表示成功，-1 表示参数错误
   * @throws IOException 作业执行失败
   */
  @Override
  public int run(String[] args) throws IOException {
    // 公共工具类：解析参数、构造 JobConf 并设置输入输出路径
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    // 返回 null 说明参数个数不对
    if (conf == null) {
      // 用法错误，返回 -1
      return -1;
    }
    
    // 读入的是 SequenceFile
    conf.setInputFormat(SequenceFileInputFormat.class);
    // 输出键是气温
    conf.setOutputKeyClass(IntWritable.class);
    // 输出成 MapFile：既能随机查找，也能顺序遍历
    /*[*/conf.setOutputFormat(MapFileOutputFormat.class);/*]*/
    // 打开输出压缩
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    // 用 Gzip 压缩
    SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    // 按块压缩
    SequenceFileOutputFormat.setOutputCompressionType(conf,
        CompressionType.BLOCK);

    // v1 由 JobClient 提交并运行作业
    JobClient.runJob(conf);
    // 运行成功
    return 0;
  }
  
  /**
   * 程序入口
   *
   * @param args 命令行参数
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会解析通用选项并调用 run
    int exitCode = ToolRunner.run(new SortByTemperatureToMapFile(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
