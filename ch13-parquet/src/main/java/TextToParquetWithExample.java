import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 * 用 Parquet 自己的 {@code ExampleOutputFormat} 把文本文件转成 Parquet 文件
 *
 * <p>Parquet 是列式存储：同一列的值连续存放，
 * 压缩率与读取效率都比行式格式好
 * <p>Parquet 提供两套读写接口：一套基于 Avro
 * （{@code AvroParquetInputFormat} / {@code AvroParquetOutputFormat}），
 * 一套是它自己的示例接口（{@code ExampleInputFormat} / {@code ExampleOutputFormat}）。
 * 本例用后者：模式用 Parquet 自己的 {@link MessageType} 来描述
 *
 * Convert text files to Parquet files using Parquet's {@code ExampleOutputFormat}.
 */
public class TextToParquetWithExample extends Configured implements Tool {

  // 写出用的模式：offset 是行号，line 是整行文本
  // MessageType 是 Parquet 自己的模式表示，与 Avro 的 Schema 各管一套
  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
      "message Line {\n" +
      "  required int64 offset;\n" +
      "  required binary line (UTF8);\n" +
      "}");

  /**
   * 把一行文本转成 Parquet 记录的 Mapper
   *
   * <p>键是行号，值是整行文本，
   * 两个字段正好对应模式里的 offset 与 line
   */
  public static class TextToParquetMapper
      extends Mapper<LongWritable, Text, Void, Group> {

    // 记录工厂：按模式来创建记录对象
    private GroupFactory groupFactory = new SimpleGroupFactory(SCHEMA);

    /**
     * 把一行文本转成一条记录并输出
     *
     * @param key 输入的键，即这一行在文件中的位置
     * @param value 输入的值，即整行文本
     * @param context 上下文，用来写出结果
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      // 新建一条记录并依次填入两个字段：
      // append 的返回值就是这条记录本身，可以一直串下去
      Group group = groupFactory.newGroup()
          .append("offset", key.get())
          .append("line", value.toString());
      // Parquet 不需要键，所以键写成 null
      context.write(null, group);
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
    Job job = new Job(getConf(), "Text to Parquet");
    // 告诉框架作业的 jar 包在哪里
    job.setJarByClass(getClass());

    // 输入路径
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // 输出路径
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // 指定 Mapper
    job.setMapperClass(TextToParquetMapper.class);
    // 不需要 Reduce：这里只是逐条转格式
    job.setNumReduceTasks(0);

    // 输出格式：写 Parquet 文件，用 Parquet 自己的示例接口
    job.setOutputFormatClass(ExampleOutputFormat.class);
    // 告诉输出格式写出时用哪个模式
    ExampleOutputFormat.setSchema(job, SCHEMA);

    // 输出键是空的
    job.setOutputKeyClass(Void.class);
    // 输出值是 Parquet 的记录
    job.setOutputValueClass(Group.class);

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
    int exitCode = ToolRunner.run(new TextToParquetWithExample(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
