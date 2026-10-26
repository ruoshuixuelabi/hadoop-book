
import java.io.IOException;

import com.cloudera.sqoop.lib.RecordParser.ParseError;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

/**
 * 用 MapReduce 找出 id 最大的 widget
 *
 * <p>这是第 15 章 Sqoop 那一节的配套示例：
 * Sqoop 把关系表导成文本之后，
 * 就可以用普通的 MapReduce 作业去分析这些数据
 * <p>做法是「Map 端各求一个最大值，Reduce 端再求一次最大值」，
 * 这是求最大值最常见的 map/reduce 两段式写法
 * <p>输入是 Sqoop 导出的 widgets 目录，
 * 每一行都是一条以逗号分隔的 widget 记录
 */
public class MaxWidgetId extends Configured implements Tool {

  /**
   * 每个 Map 任务各自记住它见到的 id 最大的那条记录
   *
   * <p>过程中不输出任何东西，
   * 等任务结束时（cleanup）再把本任务的最大值写出去一条，
   * 这样中间结果就只有 Map 任务数那么多
   */
  public static class MaxWidgetMapper
      extends Mapper<LongWritable, Text, LongWritable, Widget> {

    // 当前见过的 id 最大的记录；一条都还没见到时是 null
    private Widget maxWidget = null;

    /**
     * 解析一条记录，记住 id 更大的那一条
     *
     * @param k 输入的键（行号），这里用不上
     * @param v 输入的值，即一行文本
     * @param context 上下文
     */
    public void map(LongWritable k, Text v, Context context) {
      // 新建一条空的记录对象
      Widget widget = new Widget();
      try {
        // 用 Sqoop 生成的解析器把一行文本拆成各个字段：
        // 格式不对时会抛 ParseError
        widget.parse(v); // Auto-generated: parse all fields from text.
      } catch (ParseError pe) {
        // Got a malformed record. Ignore it.
        return;
      }

      // 取出 id 字段
      Integer id = widget.get_id();
      // id 为空的记录没法参与比较，跳过
      if (null == id) {
        return;
      } else {
        // 还没有记录，或者这一条比手上的那条更大
        if (maxWidget == null
            || id.intValue() > maxWidget.get_id().intValue()) {
          // 记下来
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
    public void cleanup(Context context)
        throws IOException, InterruptedException {
      // 一个记录都没有的话就没得写
      if (null != maxWidget) {
        context.write(new LongWritable(0), maxWidget);
      }
    }
  }

  /**
   * 把各个 Map 任务的结果合并成一个最大值
   */
  public static class MaxWidgetReducer
      extends Reducer<LongWritable, Widget, Widget, NullWritable> {

    // There will be a single reduce call with key '0' which gets
    // the max widget from each map task. Pick the max widget from
    // this list.
    // 也就是说：所有 Map 任务的输出都汇总到同一个键 '0' 上，
    // Reduce 端只需要从这一组值里挑出最大的那个

    /**
     * 从这一组记录里挑出 id 最大的那条
     *
     * @param k 输入的键，恒为 0
     * @param vals 所有 Map 任务写出来的记录
     * @param context 上下文
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    public void reduce(LongWritable k, Iterable<Widget> vals, Context context)
        throws IOException, InterruptedException {
      // 目前的最大值
      Widget maxWidget = null;

      // 逐条比较
      for (Widget w : vals) {
        // 还没有记录，或者这一条更大
        if (maxWidget == null
            || w.get_id().intValue() > maxWidget.get_id().intValue()) {
          // 复制一份再保存：
          // Reduce 端拿到的迭代器有可能复用同一个对象
          try {
            maxWidget = (Widget) w.clone();
          } catch (CloneNotSupportedException cnse) {
            // Shouldn't happen; Sqoop-generated classes support clone().
            throw new IOException(cnse);
          }
        }
      }

      // 没有任何输入时就不输出
      if (null != maxWidget) {
        // 整个作业只要一条记录，所以键是记录本身，值是空的
        context.write(maxWidget, NullWritable.get());
      }
    }
  }

  /**
   * 运行作业
   *
   * @param args 命令行参数（这个例子没有用到）
   * @return 0 表示成功，1 表示作业失败
   * @throws Exception 作业执行失败
   */
  public int run(String [] args) throws Exception {
    // 新建作业
    Job job = new Job(getConf());

    // 告诉框架作业的 jar 包在哪里
    job.setJarByClass(MaxWidgetId.class);

    // 指定 Mapper
    job.setMapperClass(MaxWidgetMapper.class);
    // 指定 Reducer
    job.setReducerClass(MaxWidgetReducer.class);

    // 输入：Sqoop 导出的 widgets 目录
    FileInputFormat.addInputPath(job, new Path("widgets"));
    // 输出目录
    FileOutputFormat.setOutputPath(job, new Path("maxwidget"));

    // Map 输出的键：所有记录都写同一个键 0，好让它们汇总到一处
    job.setMapOutputKeyClass(LongWritable.class);
    // Map 输出的值：widget 记录本身
    job.setMapOutputValueClass(Widget.class);

    // 最终输出的键：就是那条最大记录
    job.setOutputKeyClass(Widget.class);
    // 最终输出的值：空的，一条记录就够了
    job.setOutputValueClass(NullWritable.class);

    // 只用一个 Reduce：所有 Map 的结果要汇总到同一个地方再求最大值
    job.setNumReduceTasks(1);

    // 提交作业并等待它跑完
    if (!job.waitForCompletion(true)) {
      // 作业失败
      return 1; // error.
    }

    return 0;
  }

  /**
   * 程序入口
   *
   * @param args 命令行参数
   * @throws Exception 作业执行失败
   */
  public static void main(String [] args) throws Exception {
    // ToolRunner 会解析通用选项并调用 run
    int ret = ToolRunner.run(new MaxWidgetId(), args);
    // 以退出码结束进程
    System.exit(ret);
  }
}
