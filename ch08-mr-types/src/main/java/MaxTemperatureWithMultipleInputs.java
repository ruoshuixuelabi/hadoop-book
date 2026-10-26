// == MaxTemperatureWithMultipleInputs
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

/**
 * 把两种格式的输入合并到同一个作业里处理
 *
 * <p>NCDC 与英国气象局（Met Office）的记录格式不同：
 * 前者是定宽文本，后者是带分隔符的 CSV
 * <p>{@link org.apache.hadoop.mapreduce.lib.input.MultipleInputs} 允许
 * 为每个输入路径单独指定输入格式与对应的 Mapper，
 * 于是两种格式的记录都能被解析成统一的「年份 → 气温」
 */
public class MaxTemperatureWithMultipleInputs extends Configured
  implements Tool {
  
  /**
   * 解析 Met Office 格式记录的 Mapper
   *
   * <p>与 {@code MaxTemperatureMapper} 的区别只在于解析器不同，
   * 其余流程完全一致
   */
  static class MetOfficeMaxTemperatureMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {
  
    // 解析器实例：一个 Map 任务只创建一次
    private MetOfficeRecordParser parser = new MetOfficeRecordParser();
    
    /**
     * 解析一条 Met Office 记录并输出
     *
     * @param key 输入的键（本例用不到）
     * @param value 输入的值，即一行 Met Office 记录
     * @param context 上下文，用来写出结果
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      // 解析器把这一行拆成年份、气温等字段
      parser.parse(value);
      // 只有读数有效的记录才输出，缺测与可疑读数一律丢弃
      if (parser.isValidTemperature()) {
        // 输出的键是年份，值是气温
        context.write(new Text(parser.getYear()),
            new IntWritable(parser.getAirTemperature()));
      }
    }
  }

  /**
   * 运行作业
   *
   * @param args 依次为 NCDC 输入、Met Office 输入、输出路径
   * @return 0 表示成功，-1 表示参数错误
   * @throws Exception 作业执行失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 本例需要两个输入路径加一个输出路径
    if (args.length != 3) {
      // 打印用法提示
      JobBuilder.printUsage(this, "<ncdc input> <metoffice input> <output>");
      // 参数错误，返回 -1
      return -1;
    }
    
    // 第二个参数是作业名，会显示在作业监控页面上
    Job job = new Job(getConf(), "Max temperature with multiple input formats");
    // 指定作业 jar 包的位置
    job.setJarByClass(getClass());
    
    // NCDC 数据目录
    Path ncdcInputPath = new Path(args[0]);
    // Met Office 数据目录
    Path metOfficeInputPath = new Path(args[1]);
    // 输出目录
    Path outputPath = new Path(args[2]);
    
// vv MaxTemperatureWithMultipleInputs    
    // 两次 addInputPath 调用分别绑定各自的输入格式与 Mapper，
    // 框架会为每个输入生成独立的 InputSplit，
    // 再按各自的 Mapper 去解析：第一个用 NCDC 的 Mapper，
    // 第二个用 Met Office 专用的 Mapper
    MultipleInputs.addInputPath(job, ncdcInputPath,
        TextInputFormat.class, MaxTemperatureMapper.class);
    MultipleInputs.addInputPath(job, metOfficeInputPath,
        TextInputFormat.class, MetOfficeMaxTemperatureMapper.class);
// ^^ MaxTemperatureWithMultipleInputs
    // 输出路径只能设置一次
    FileOutputFormat.setOutputPath(job, outputPath);
    
    // 输出键是年份
    job.setOutputKeyClass(Text.class);
    // 输出值是气温
    job.setOutputValueClass(IntWritable.class);
    
    // 在 Map 与 Reduce 之间先做一次合并，可以显著减少网络传输量
    job.setCombinerClass(MaxTemperatureReducer.class);
    // 真正的全局聚合仍由 Reducer 完成
    job.setReducerClass(MaxTemperatureReducer.class);

    // 提交作业并等待它跑完
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  /**
   * 程序入口
   *
   * @param args 命令行参数
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会解析通用选项并调用 run
    int exitCode = ToolRunner.run(new MaxTemperatureWithMultipleInputs(),
        args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
