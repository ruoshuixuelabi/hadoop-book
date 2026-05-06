/*
 * 本文件演示了如何使用Sqoop导入的Widget对象查找最大ID
 *
 * 应用场景：
 * - Sqoop从MySQL数据库导出了widgets表到HDFS
 * - 导出的数据格式是逗号分隔的文本文件，每行代表一条记录
 * - 本程序找出所有widget中ID最大的那条记录
 *
 * 工作流程：
 * 1. Mapper阶段：读取每个widget记录，追踪当前map任务中的最大widget
 * 2. Reducer阶段：从所有mapper的输出中找出全局最大ID的widget
 * 3. 输出：最大ID的widget记录
 */

// cc imports - Sqoop特定的导入，用于解析Sqoop生成的Record
import java.io.IOException;

import com.cloudera.sqoop.lib.RecordParser.ParseError;

// vv Hadoop核心库导入
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

/**
 * 从Sqoop导入的widget数据中找出最大ID的widget记录。
 * <p>
 * 本类实现MapReduce模式来处理这个问题：<br/>
 * - Map阶段：每个mapper找出其处理数据中的最大widget<br/>
 * - Reduce阶段：从所有mapper的结果中选出全局最大ID的widget
 * <p>
 * 设计要点：<br/>
 * 1. 使用Sqoop生成的Widget类（继承自SqoopRecord）来表示数据记录<br/>
 * 2. Widget类实现了DBWritable和Writable接口，支持Hadoop序列化<br/>
 * 3. 在map任务cleanup阶段输出当前map的最大widget，减少网络传输
 * <p>
 * 使用示例：
 * <pre>
 * hadoop jar sqoop-examples.jar MaxWidgetId
 * # 输入：HDFS上的widgets目录（包含Sqoop导出的文本文件）
 * # 输出：maxwidget目录，包含最大ID的widget记录
 * </pre>
 *
 * @see Widget
 * @see MaxWidgetIdGenericAvro
 */
public class MaxWidgetId extends Configured implements Tool {

    /**
     * Mapper类：负责在map任务内找出最大ID的widget。
     * <p/>
     * 输入：LongWritable（行偏移量）, Text（原始行数据）<br/>
     * 输出：LongWritable（固定key=0）, Widget（当前map内的最大widget）
     * <p/>
     * 设计说明：<br/>
     * - 使用固定key=0将所有mapper的输出发送到同一个reducer<br/>
     * - 这样reducer可以方便地比较所有mapper的结果<br/>
     * - maxWidget在map方法中持续更新，在cleanup中一次性输出
     */
    public static class MaxWidgetMapper
            extends Mapper<LongWritable, Text, LongWritable, Widget> {

        // 存储当前map任务中的最大widget，初始为null
        private Widget maxWidget = null;

        /**
         * Map方法：处理每一行文本数据。
         * <p/>
         * 调用Sqoop生成的parse方法，自动解析所有字段<br/>
         * 解析格式为：id,widget_name,price,design_date,version,design_comment
         *
         * @param key 行在文件中的偏移量（本例中未使用）
         * @param v 原始文本行，内容为逗号分隔的widget字段
         * @param context MapReduce上下文，用于输出结果
         */
        public void map(LongWritable k, Text v, Context context) {
            Widget widget = new Widget();
            try {
                // 调用Sqoop生成的parse方法，自动解析所有字段
                // 解析格式为：id,widget_name,price,design_date,version,design_comment
                widget.parse(v); // Auto-generated: parse all fields from text.
            } catch (ParseError pe) {
                // 遇到格式错误的记录，跳过该行
                // 使用计数器可以在Job完成后查看解析失败的记录数
                return;
            }

            Integer id = widget.get_id();
            // 忽略id为null的记录
            if (null == id) {
                return;
            } else {
                // 比较当前widget的id与已知的最大值，更新maxWidget
                if (maxWidget == null
                        || id.intValue() > maxWidget.get_id().intValue()) {
                    maxWidget = widget;
                }
            }
        }

        /**
         * Cleanup方法：在map任务结束前输出当前map内的最大widget。
         * <p/>
         * 选择在cleanup而不是map中逐条输出，是因为：<br/>
         * 1. 减少输出数据量（每个map只输出1条而不是数万条）<br/>
         * 2. 减少shuffle阶段的网络传输<br/>
         * 3. 保证每个map只贡献一个widget到reducer
         *
         * @param context MapReduce上下文
         * @throws IOException 如果输出发生I/O错误
         * @throws InterruptedException 如果操作被中断
         */
        public void cleanup(Context context)
                throws IOException, InterruptedException {
            if (null != maxWidget) {
                // 使用固定key=0，使得所有mapper的输出发送到同一个reducer
                context.write(new LongWritable(0), maxWidget);
            }
        }
    }

    /**
     * Reducer类：负责从所有mapper的结果中找出全局最大ID的widget。
     * <p/>
     * 输入：LongWritable（固定key=0）, Iterable<Widget>（来自所有mapper的最大widget列表）<br/>
     * 输出：Widget（全局最大ID的widget）, NullWritable
     * <p/>
     * 设计说明：<br/>
     * - 由于所有mapper都使用key=0，reducer只会收到一个key对应的值迭代器<br/>
     * - 使用clone方法复制widget对象，避免迭代器返回的同一对象被覆盖
     */
    public static class MaxWidgetReducer
            extends Reducer<LongWritable, Widget, Widget, NullWritable> {

        // There will be a single reduce call with key '0' which gets
        // the max widget from each map task. Pick the max widget from
        // this list.
        /**
         * Reduce方法：遍历所有mapper传来的widget，找出最大ID。
         * <p/>
         * 遍历所有mapper传来的widget，找出最大ID<br/>
         * 必须clone，因为迭代器可能返回同一个对象
         *
         * @param k 固定值0，表示所有mapper都使用相同key
         * @param vals 所有mapper输出的最大widget迭代器
         * @param context 上下文，用于输出最终结果
         */
        public void reduce(LongWritable k, Iterable<Widget> vals, Context context)
                throws IOException, InterruptedException {
            Widget maxWidget = null;

            for (Widget w : vals) {
                if (maxWidget == null
                        || w.get_id().intValue() > maxWidget.get_id().intValue()) {
                    try {
                        // 必须clone，因为迭代器可能返回同一个对象
                        // Sqoop生成的类都实现了Cloneable接口
                        maxWidget = (Widget) w.clone();
                    } catch (CloneNotSupportedException cnse) {
                        // Shouldn't happen; Sqoop-generated classes support clone().
                        throw new IOException(cnse);
                    }
                }
            }

            // 输出最大widget，value使用NullWritable因为widget本身已包含所有数据
            if (null != maxWidget) {
                context.write(maxWidget, NullWritable.get());
            }
        }
    }

    /**
     * 配置并执行MapReduce作业。
     * <p/>
     * 作业配置要点：<br/>
     * - 设置输入路径为"widgets"目录（需要预先存在Sqoop导入的数据）<br/>
     * - 设置输出路径为"maxwidget"目录<br/>
     * - 使用单个reducer确保最终只输出一个最大widget
     *
     * @param args 命令行参数（本程序不使用，通过硬编码配置路径）
     * @return 作业成功返回0，失败返回1
     * @throws Exception 如果作业配置或执行出错
     */
    public int run(String [] args) throws Exception {
        Job job = new Job(getConf());

        job.setJarByClass(MaxWidgetId.class);

        // 设置自定义的Mapper和Reducer类
        job.setMapperClass(MaxWidgetMapper.class);
        job.setReducerClass(MaxWidgetReducer.class);

        // 设置输入输出路径（硬编码为widgets和maxwidget目录）
        FileInputFormat.addInputPath(job, new Path("widgets"));
        FileOutputFormat.setOutputPath(job, new Path("maxwidget"));

        // 设置map阶段的输出类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Widget.class);

        // 设置最终输出类型
        job.setOutputKeyClass(Widget.class);
        job.setOutputValueClass(NullWritable.class);

        // 使用单个reducer确保最终只有一个最大widget输出
        job.setNumReduceTasks(1);

        // 等待作业完成，返回是否成功
        if (!job.waitForCompletion(true)) {
            return 1; // error.
        }

        return 0;
    }

    /**
     * 程序入口：使用ToolRunner运行本作业。
     * <p/>
     * ToolRunner是Hadoop提供的工具类，简化了Tool接口的实现<br/>
     * 它会自动处理配置文件的加载和命令行参数的解析
     *
     * @param args 命令行参数
     * @throws Exception 如果程序执行出错
     */
    public static void main(String [] args) throws Exception {
        int ret = ToolRunner.run(new MaxWidgetId(), args);
        System.exit(ret);
    }
}
