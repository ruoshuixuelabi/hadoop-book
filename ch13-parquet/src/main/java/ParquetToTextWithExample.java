import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleInputFormat;

/**
 * 用 Parquet 自己的 {@code ExampleInputFormat} 把 Parquet 文件转成文本
 *
 * <p>Parquet 是列式存储：同一列的值连续存放，
 * 所以只读其中几列时不必把整行都读出来
 * <p>Parquet 提供两套读写接口：一套基于 Avro
 * （{@code AvroParquetInputFormat} / {@code AvroParquetOutputFormat}），
 * 一套是它自己的示例接口（{@code ExampleInputFormat} / {@code ExampleOutputFormat}）。
 * 本例用后者：读出来的是 Parquet 自己的 {@link Group} 对象，
 * 打印出来的样子与 Avro 那套不同
 *
 * Convert Parquet files to text using Parquet's {@code ExampleInputFormat}.
 */
public class ParquetToTextWithExample extends Configured implements Tool {

  /**
   * 把一条 Parquet 记录转成文本的 Mapper
   *
   * <p>读出来的是 Parquet 的 {@link Group}：
   * 字段用「名字 + 序号」来定位
   */
  public static class ParquetToTextMapper
      extends Mapper<Void, Group, NullWritable, Text> {

    /**
     * 把一条记录转成文本输出
     *
     * @param key 输入的键（Parquet 不需要键，这里是空的）
     * @param value 输入的记录
     * @param context 上下文，用来写出结果
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void map(Void key, Group value, Context context)
        throws IOException, InterruptedException {
      // Group 的 toString 会按「字段名: 值」逐行打印
      context.write(NullWritable.get(), new Text(value.toString()));
    }
  }

  /**
   * 运行作业
   *
   * @param args 依次为输入路径与输出路径
   * @return 0 表示成功，-1 表示参数错误
   * @throws Exception 作业执行失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 需要一个输入路径加一个输出路径
    if (args.length != 2) {
      // 打印用法提示
      // 第二个参数是类名
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      // 再说明一遍通用选项
      ToolRunner.printGenericCommandUsage(System.err);
      // 参数错误，返回 -1
      return -1;
    }

    // 新建作业
    Job job = new Job(getConf(), "Parquet to text");
    // 告诉框架作业的 jar 包在哪里
    job.setJarByClass(getClass());

    // 输入路径
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // 输出路径
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // 指定 Mapper
    job.setMapperClass(ParquetToTextMapper.class);
    // 不需要 Reduce：这里只是逐条转格式
    job.setNumReduceTasks(0);

    // 输入格式：读 Parquet 文件，用 Parquet 自己的示例接口
    job.setInputFormatClass(ExampleInputFormat.class);

    // 输出键是空的
    job.setOutputKeyClass(NullWritable.class);
    // 输出值是转成文本后的记录
    job.setOutputValueClass(Text.class);

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
    int exitCode = ToolRunner.run(new ParquetToTextWithExample(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
