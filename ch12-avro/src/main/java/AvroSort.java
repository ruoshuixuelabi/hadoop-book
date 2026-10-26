// cc AvroSort A MapReduce program to sort an Avro data file

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//vv AvroSort
/**
 * 对一个 Avro 数据文件做全局排序
 *
 * <p>输入是一个 Avro 容器文件，键就是文件里排好序的记录；
 * 本例用 Map/Reduce 重新排一遍
 * <p>之所以用泛型参数 {@code K}，
 * 是因为同一段代码既能给字符串键排序，也能给别的类型排序
 */
public class AvroSort extends Configured implements Tool {

  /**
   * 把 Avro 键原样转发出去的 Mapper
   *
   * <p>输入的键已经带着值，值是空的；
   * 这里把值补上，交给 Reducer 统一输出
   */
  static class SortMapper<K> extends Mapper<AvroKey<K>, NullWritable,
      AvroKey<K>, AvroValue<K>> {
    /**
     * 把键与值配成一对输出
     *
     * @param key 输入的 Avro 键
     * @param value 输入的值（本例为空）
     * @param context 上下文，用来写出结果
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void map(AvroKey<K> key, NullWritable value,
        Context context) throws IOException, InterruptedException {
      // 键不变，值取自键里的数据
      context.write(key, new AvroValue<K>(key.datum()));
    }
  }

  /**
   * 把组里的记录逐条输出的 Reducer
   *
   * <p>框架已经把同键的记录聚到一起并排好序，
   * 这里只要按顺序写出去
   */
  static class SortReducer<K> extends Reducer<AvroKey<K>, AvroValue<K>,
      AvroKey<K>, NullWritable> {
    /**
     * 按顺序输出本组的所有记录
     *
     * @param key 分组键
     * @param values 本组的所有值
     * @param context 上下文，用来写出结果
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void reduce(AvroKey<K> key, Iterable<AvroValue<K>> values,
        Context context) throws IOException, InterruptedException {
      // 遍历本组的记录
      for (AvroValue<K> value : values) {
        // 键是记录本身，值没有意义
        context.write(new AvroKey(value.datum()), NullWritable.get());
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

    // 新建作业
    Job job = new Job(getConf(), "Avro sort");
    // 告诉框架作业的 jar 包在哪里
    job.setJarByClass(getClass());

    // 打开这个开关，作业才会优先使用用户 jar 包里的类
    // Avro 的读写类必须来自作业自己的 jar 包
    job.getConfiguration().setBoolean(
        Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

    // 输入路径
    FileInputFormat.addInputPath(job, new Path(input));
    // 输出路径
    FileOutputFormat.setOutputPath(job, new Path(output));

    // 用通用数据模型：键值都是通用的对象，不绑定具体类
    AvroJob.setDataModelClass(job, GenericData.class);

    // 从模式文件里读出模式
    Schema schema = new Schema.Parser().parse(new File(schemaFile));
    // 输入的键是这个模式
    AvroJob.setInputKeySchema(job, schema);
    // Map 输出的键也是这个模式
    AvroJob.setMapOutputKeySchema(job, schema);
    // Map 输出的值同样是这个模式
    AvroJob.setMapOutputValueSchema(job, schema);
    // 最终输出的键也是这个模式
    AvroJob.setOutputKeySchema(job, schema);

    // 输入格式：读 Avro 容器文件，取出它的键
    job.setInputFormatClass(AvroKeyInputFormat.class);
    // 输出格式：写 Avro 容器文件
    job.setOutputFormatClass(AvroKeyOutputFormat.class);

    // 输出键的运行时类型
    job.setOutputKeyClass(AvroKey.class);
    // 输出值是空的
    job.setOutputValueClass(NullWritable.class);

    // 指定 Mapper
    job.setMapperClass(SortMapper.class);
    // 指定 Reducer
    job.setReducerClass(SortReducer.class);

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
    int exitCode = ToolRunner.run(new AvroSort(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
// ^^ AvroSort
