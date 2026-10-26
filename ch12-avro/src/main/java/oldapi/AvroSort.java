package oldapi;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 对一个 Avro 数据文件做全局排序（v1 版）
 *
 * <p>输入是一个 Avro 容器文件，键就是文件里排好序的记录；
 * 本例用 Map/Reduce 重新排一遍
 * <p>因为 v1 的 Avro 接口只能输出「键 + 值」，
 * 所以中间结果要先打包成 {@link Pair}，到了 Reducer 再拆开
 */
public class AvroSort extends Configured implements Tool {

  /**
   * 把 Avro 键原样转发出去的 Mapper
   *
   * <p>输入的键已经带着值，值是空的；
   * 这里把值补上，交给 Reducer 统一输出
   */
  static class SortMapper<K> extends AvroMapper<K, Pair<K, K>> {
    /**
     * 把键与值配成一对输出
     *
     * @param datum 输入的键
     * @param collector 结果收集器
     * @param reporter 状态报告器
     * @throws IOException 写出失败
     */
    public void map(K datum, AvroCollector<Pair<K, K>> collector,
        Reporter reporter) throws IOException {
      // Pair 的两个分量各有一份模式，这里两侧都用键的模式
      collector.collect(new Pair<K, K>(datum, null, datum, null));
    }
  }

  /**
   * 把组里的记录逐条输出的 Reducer
   *
   * <p>框架已经把同键的记录聚到一起并排好序，
   * 这里只要按顺序写出去
   */
  static class SortReducer<K> extends AvroReducer<K, K, K> {
    /**
     * 按顺序输出本组的所有记录
     *
     * @param key 分组键
     * @param values 本组的所有值
     * @param collector 结果收集器
     * @param reporter 状态报告器
     * @throws IOException 写出失败
     */
    public void reduce(K key, Iterable<K> values,
        AvroCollector<K> collector,
        Reporter reporter) throws IOException {
      // 遍历本组的记录
      for (K value : values) {
        // 直接输出记录本身
        collector.collect(value);
      }
    }
  }

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
      // 说明三个参数的含义，第二个是类名
      System.err.printf(
        "Usage: %s [generic options] <input> <output> <schema-file>\n",
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
    conf.setJobName("Avro sort");

    // 用通用数据模型：键值都是通用的对象，不绑定具体类
    AvroJob.setDataModelClass(conf, GenericData.class);

    // 输入路径
    FileInputFormat.addInputPath(conf, new Path(input));
    // 输出路径
    FileOutputFormat.setOutputPath(conf, new Path(output));

    // 从模式文件里读出模式
    Schema schema = new Schema.Parser().parse(new File(schemaFile));
    // 输入的键是这个模式
    AvroJob.setInputSchema(conf, schema);
    // 中间结果是「键 + 值」，两侧都用同一个模式
    Schema intermediateSchema = Pair.getPairSchema(schema, schema);
    // 设置中间结果的模式
    AvroJob.setMapOutputSchema(conf, intermediateSchema);
    // 最终输出的键是这个模式
    AvroJob.setOutputSchema(conf, schema);
    
    // 指定 Mapper
    AvroJob.setMapperClass(conf, SortMapper.class);
    // 指定 Reducer
    AvroJob.setReducerClass(conf, SortReducer.class);
  
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
    int exitCode = ToolRunner.run(new AvroSort(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
