package oldapi;

import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 按新的模式重写一个 Avro 文件（v1 版）
 *
 * <p>Avro 的模式可以互相转换：
 * 用「写出模式」写、再用「读入模式」读，
 * 就能把一个文件投影成另一种结构
 * <p>本例只做 Map 阶段（Reduce 任务数设为 0），
 * 因为「逐条改写」不需要分组聚合
 */
public class AvroProjection extends Configured implements Tool {

  /**
   * 运行作业
   *
   * @param args 依次为输入路径、输出路径与模式文件
   * @return 0 表示成功，-1 表示参数错误
   * @throws Exception 作业执行失败
   */
  @Override
  public int run(String[] args) throws Exception {
    
    // 需要输入、输出与模式文件三个参数
    if (args.length != 3) {
      // 打印用法提示
      // 第二个参数是类名
      System.err.printf("Usage: %s [generic options] <input> <output> <schema-file>\n",
          getClass().getSimpleName());
      // 再说明一遍通用选项
      ToolRunner.printGenericCommandUsage(System.err);
      // 参数错误，返回 -1
      return -1;
    }
    
    // 输入路径
    String input = args[0];
    // 输出路径
    String output = args[1];
    // 模式文件路径
    String schemaFile = args[2];

    // v1 用 JobConf 承载全部配置
    JobConf conf = new JobConf(getConf(), getClass());
    // 设置作业名
    conf.setJobName("Avro projection");
    
    // 输入路径
    FileInputFormat.addInputPath(conf, new Path(input));
    // 输出路径
    FileOutputFormat.setOutputPath(conf, new Path(output));
    
    // 从模式文件里读出模式
    Schema schema = new Schema.Parser().parse(new File(schemaFile));
    // 输入的键是这个模式
    AvroJob.setInputSchema(conf, schema);
    // Map 输出的键也是这个模式
    AvroJob.setMapOutputSchema(conf, schema);
    // 最终输出的键还是这个模式
    AvroJob.setOutputSchema(conf, schema);
    // 不需要 Reduce：只做逐条改写
    conf.setNumReduceTasks(0);

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
    int exitCode = ToolRunner.run(new AvroProjection(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }

}
