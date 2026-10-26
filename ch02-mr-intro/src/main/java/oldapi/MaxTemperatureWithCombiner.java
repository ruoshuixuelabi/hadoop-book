package oldapi;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * 统计每个气象站最高气温并使用 Combiner 提前归约（MapReduce v1 旧 API 版本）
 *
 * <p>Combiner 在 Map 端做局部归约，能显著减少 Shuffle 阶段的网络传输量；
 * 它要求聚合函数满足结合律与交换律，「求最大值」满足这两个条件
 */
public class MaxTemperatureWithCombiner {

  /**
   * 作业入口：用 v1 风格的 JobConf 组装并提交带 Combiner 的作业
   *
   * @param args 恰好两个参数：输入路径、输出路径
   * @throws IOException 配置或运行作业时发生 I/O 错误
   */
  public static void main(String[] args) throws IOException {
    // 参数个数校验
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCombiner <input path> " +
      		"<output path>");
      System.exit(-1);
    }
    
    JobConf conf = new JobConf(MaxTemperatureWithCombiner.class);
    conf.setJobName("Max temperature");

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setMapperClass(MaxTemperatureMapper.class);
    /*[*/conf.setCombinerClass(MaxTemperatureReducer.class)/*]*/;
    conf.setReducerClass(MaxTemperatureReducer.class);

    // 显式声明输出键值类型
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    // v1 直接提交并等待作业结束
    JobClient.runJob(conf);
  }
}
