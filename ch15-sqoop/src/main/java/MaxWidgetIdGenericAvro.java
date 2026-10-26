import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 用 Avro 记录做 MapReduce，找出 id 最大的 widget
 *
 * <p>和 {@link MaxWidgetId} 求的是同一个最大值，
 * 区别在输入输出的格式：这里读的是 Sqoop 以 Avro 格式导出的文件，
 * 键和值都用 {@code AvroKey} / {@code AvroValue} 包着，
 * 于是整个作业都可以按 Avro 的模式来描述
 * <p>代价是：作业的输入模式必须事先从文件里读出来，
 * 而 {@code readSchema} 只能打开输入目录里的第一个文件
 */
public class MaxWidgetIdGenericAvro extends Configured implements Tool {

  /**
   * 每个 Map 任务各自记住它见到的 id 最大的那条记录
   */
  public static class MaxWidgetMapper extends Mapper<AvroKey<GenericRecord>, NullWritable,
      AvroKey<Long>, AvroValue<GenericRecord>> {

    // 当前见过的 id 最大的记录
    private GenericRecord maxWidget;
    /**
     * 解析一条记录，记住 id 更大的那一条
     *
     * @param key 输入的键，datum() 里才是真正的记录
     * @param value 输入的值，这里是空的
     * @param context 上下文
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
        throws IOException, InterruptedException {
      // 从 AvroKey 里取出记录本身
      GenericRecord widget = key.datum();
      // 按字段名取值：通用模式下字段是按名字取的
      Integer id = (Integer) widget.get("id");
      // id 为空的记录没法比较，跳过
      if (id != null) {
        // 还没有记录，或者这一条更大
        if (maxWidget == null
            || id > (Integer) maxWidget.get("id")) {
          maxWidget = widget;
        }
      }
    }

    /**
     * Map 任务结束时，把本任务找到的最大值写出去
     *
     * @param context 上下文
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      // 键是 0L，好让所有 Map 的结果汇总到同一个 Reduce 键上
      context.write(new AvroKey(0L), new AvroValue<GenericRecord>(maxWidget));
    }
  }

  /**
   * 把各个 Map 任务的结果合并成一个最大值
   */
  public static class MaxWidgetReducer
      extends Reducer<AvroKey<Long>, AvroValue<GenericRecord>,
      AvroKey<GenericRecord>, NullWritable> {

    /**
     * 从这一组记录里挑出 id 最大的那条
     *
     * @param key 输入的键，恒为 0
     * @param values 所有 Map 任务写出来的记录
     * @param context 上下文
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void reduce(AvroKey<Long> key, Iterable<AvroValue<GenericRecord>>
        values, Context context) throws IOException, InterruptedException {
      // 目前的最大值
      GenericRecord maxWidget = null;
      // 逐条比较
      for (AvroValue<GenericRecord> value : values) {
        // 从 AvroValue 里取出记录本身
        GenericRecord record = value.datum();
        // 还没有记录，或者这一条更大
        if (maxWidget == null
            || (Integer) record.get("id") > (Integer) maxWidget.get("id")) {
          // 复制一份再保存：迭代器有可能复用同一个对象
          maxWidget = copy(record);
        }
      }
      // 整个作业只要一条记录，所以键是记录本身，值是空的
      context.write(new AvroKey(maxWidget), NullWritable.get());
    }

    /**
     * 按模式逐字段复制一条记录
     *
     * <p>通用模式（{@link GenericRecord}）本身没有实现 {@code clone()}，
     * 所以这里照着模式里的字段一个一个搬过去
     *
     * @param record 要复制的记录
     * @return 复制出来的新记录
     */
    private GenericRecord copy(GenericRecord record) {
      // 模式就是这条记录自己的模式
      Schema schema = record.getSchema();
      // 按同一个模式建一条空记录
      GenericRecord copy = new GenericData.Record(schema);
      // 逐个字段搬过去
      for (Schema.Field f : schema.getFields()) {
        copy.put(f.name(), record.get(f.name()));
      }
      // 返回复制结果
      return copy;
    }
  }

  /**
   * 运行作业
   *
   * @param args 命令行参数（这个例子没有用到）
   * @return 0 表示成功，1 表示作业失败
   * @throws Exception 作业执行失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 新建作业
    Job job = new Job(getConf(), "Max widget ID");
    // 告诉框架作业的 jar 包在哪里
    job.setJarByClass(getClass());

    // 让用户自己的 jar 包排在前面：
    // 否则框架自带的 Avro 版本可能先被加载，版本对不上
    job.getConfiguration().setBoolean(
        Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

    // 输入：Sqoop 以 Avro 格式导出的 widgets 目录
    Path inputDir = new Path("widgets");
    // 登记输入路径
    FileInputFormat.addInputPath(job, inputDir);
    // 输出目录
    FileOutputFormat.setOutputPath(job, new Path("maxwidget"));

    // 先把输入的模式读出来：Avro 的读写都要靠它
    Schema schema = readSchema(inputDir, getConf());

    // Map 输出的键模式：一个 long
    AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.LONG));
    // Map 输出的值模式：就是输入的模式
    AvroJob.setMapOutputValueSchema(job, schema);
    // 最终输出的键模式：那条最大记录
    AvroJob.setOutputKeySchema(job, schema);

    // 输入格式：按 Avro 的方式读文件
    job.setInputFormatClass(AvroKeyInputFormat.class);
    // 输出格式：按 Avro 的方式写文件
    job.setOutputFormatClass(AvroKeyOutputFormat.class);

    // 指定 Mapper
    job.setMapperClass(MaxWidgetMapper.class);
    // 指定 Reducer
    job.setReducerClass(MaxWidgetReducer.class);

    // 提交作业并等待它跑完
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  /**
   * 从输入目录的第一个文件里读出 Avro 模式
   *
   * <p>Avro 的文件自带模式，但作业在启动 Map/Reduce 之前
   * 就需要知道模式，所以这里先打开第一个文件把模式取出来
   *
   * Read the Avro schema from the first file in the input directory.
   *
   * @param inputDir 输入目录
   * @param conf 作业配置
   * @return 读到的模式
   * @throws IOException 打开文件失败
   */
  private Schema readSchema(Path inputDir, Configuration conf) throws IOException {
    // 打开输入文件
    FsInput fsInput = null;
    // Avro 的文件读取器
    FileReader<Object> reader = null;
    try {
      // Map 任务的输出文件名是固定的，所以第一个文件就叫这个名字
      fsInput = new FsInput(new Path(inputDir, "part-m-00000.avro"), conf);
      // 用通用模式的数据读取器打开
      reader = DataFileReader.openReader(fsInput, new GenericDatumReader<Object>());
      // 模式就在文件头里
      return reader.getSchema();
    } finally {
      // 无论成功与否都要把文件关掉
      IOUtils.closeStream(fsInput);
      // 读取器也要关
      IOUtils.closeStream(reader);
    }
  }

  /**
   * 程序入口
   *
   * @param args 命令行参数
   * @throws Exception 作业执行失败
   */
  public static void main(String [] args) throws Exception {
    // ToolRunner 会解析通用选项并调用 run
    int ret = ToolRunner.run(new MaxWidgetIdGenericAvro(), args);
    // 以退出码结束进程
    System.exit(ret);
  }
}
