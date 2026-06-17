package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 数据预处理工具: 清洗气象数据并输出为SequenceFile格式.
 *
 * <p>该工具是MapReduce作业,用于处理NCDC原始气象数据:
 * <ul>
 *   <li>过滤无效的气温记录(缺失值或质量码不合格)</li>
 *   <li>将处理结果以SequenceFile格式存储,支持压缩</li>
 * </ul>
 *
 * <p>主要功能:
 * <ol>
 *   <li>读取原始文本格式的NCDC气象数据</li>
 *   <li>使用NcdcRecordParser解析每条记录</li>
 *   <li>过滤掉无效的气温数据(质量码不符合[01459])</li>
 *   <li>输出为SequenceFile二进制格式,便于后续MapReduce直接读取</li>
 * </ol>
 *
 * <p>输出配置:
 * <ul>
 *   <li>格式: SequenceFile(键值对二进制文件)</li>
 *   <li>压缩类型: BLOCK压缩(整块压缩,支持分块解压)</li>
 *   <li>压缩编码: GzipCodec</li>
 *   <li>键类型: IntWritable(气温值)</li>
 *   <li>值类型: Text(原始数据行)</li>
 * </ul>
 *
 * <p>设计目的:
 * 原始NCDC数据是固定宽度的文本格式,直接用于MapReduce效率较低.
 * 该工具将数据预处理为SequenceFile格式,可以获得更好的读写性能,
 * 并且支持块级压缩,进一步减少存储空间和IO时间.
 *
 * <p>用法示例:
 * <pre>
 * hadoop jar hadoop-examples.jar SortDataPreprocessor /ncdc/raw /ncdc/cleaned
 * </pre>
 *
 * @see NcdcRecordParser
 */
public class SortDataPreprocessor extends Configured implements Tool {

  /**
   * 内部Mapper类: 解析并过滤NCDC气象记录.
   *
   * <p>该Mapper继承MapReduceBase(旧API的抽象基类),
   * 将原始文本行解析后,输出有效的气温记录.
   *
   * <p>输入: &lt;行偏移, 原始数据行&gt;
   * <p>输出: &lt;气温值, 原始数据行&gt;
   *
   * <p>注意: 没有Reduce阶段(numReduceTasks=0),
   * Mapper直接输出结果.
   */
  static class CleanerMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, IntWritable, Text> {

    /**
     * NCDC记录解析器,用于解析固定宽度的气象数据格式.
     */
    private NcdcRecordParser parser = new NcdcRecordParser();

    /**
     * Map任务处理方法: 解析行数据并过滤无效记录.
     *
     * <p>处理流程:
     * <ol>
     *   <li>使用NcdcRecordParser解析原始行</li>
     *   <li>检查气温值是否有效</li>
     *   <li>有效则输出(气温, 原始行)键值对</li>
     * </ol>
     *
     * @param key 行在文件中的偏移量(未使用)
     * @param value 原始NCDC气象数据行
     * @param output 输出&lt;气温, 原始行&gt;键值对
     * @param reporter 作业Reporter
     * @throws IOException 如果输出时发生IO错误
     */
    public void map(LongWritable key, Text value,
        OutputCollector<IntWritable, Text> output, Reporter reporter)
        throws IOException {

      // 使用NcdcRecordParser解析原始数据行
      parser.parse(value);
      // 仅输出有效气温的记录(过滤掉9999等缺失值和质量码不合格的记录)
      if (parser.isValidTemperature()) {
        output.collect(new IntWritable(parser.getAirTemperature()), value);
      }
    }
  }

  /**
   * 执行数据预处理作业.
   *
   * <p>该方法配置并运行MapReduce作业:
   * <ol>
   *   <li>解析命令行参数获取输入输出路径</li>
   *   <li>配置Mapper类(无Reducer)</li>
   *   <li>配置输出格式为SequenceFile</li>
   *   <li>设置BLOCK压缩和Gzip编码器</li>
   *   <li>提交作业并等待完成</li>
   * </ol>
   *
   * @param args 命令行参数,args[0]为输入路径,args[1]为输出路径
   * @return 作业成功返回0,配置失败返回-1
   * @throws IOException 如果作业执行发生IO错误
   */
  @Override
  public int run(String[] args) throws IOException {
    // 使用JobBuilder解析输入输出路径并创建JobConf
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }

    // 设置Mapper类(负责数据清洗)
    conf.setMapperClass(CleanerMapper.class);

    // 设置输出键类型为IntWritable(气温值)
    // 用于排序和分区
    conf.setOutputKeyClass(IntWritable.class);
    // 设置输出值类型为Text(保留原始数据行)
    conf.setOutputValueClass(Text.class);

    // 禁用Reduce阶段: 清洗操作只需要Map
    // 设置为0表示没有Reduce任务
    conf.setNumReduceTasks(0);

    // 设置输出格式为SequenceFile(二进制键值对文件)
    conf.setOutputFormat(SequenceFileOutputFormat.class);

    // 启用SequenceFile输出压缩
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    // 指定Gzip压缩编码器
    SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    // 设置BLOCK压缩类型: 将多条记录压缩为一个块
    // BLOCK压缩比行压缩有更好的压缩率,因为可以利用记录间的冗余
    SequenceFileOutputFormat.setOutputCompressionType(conf,
        CompressionType.BLOCK);

    // 运行作业并返回状态码
    JobClient.runJob(conf);
    return 0;
  }

  /**
   * 程序主入口,使用ToolRunner运行预处理作业.
   *
   * <p>ToolRunner提供标准的Hadoop工具运行框架,支持:
   * <ul>
   *   <li>GenericOptionsParser解析通用选项(如-conf, -D等)</li>
   *   <li>Tool接口的run方法调用</li>
   *   <li>错误处理和退出码传播</li>
   * </ul>
   *
   * @param args 命令行参数
   * @throws Exception 如果执行发生异常
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner封装了Tool的运行逻辑,自动处理配置和错误
    int exitCode = ToolRunner.run(new SortDataPreprocessor(), args);
    System.exit(exitCode);
  }
}