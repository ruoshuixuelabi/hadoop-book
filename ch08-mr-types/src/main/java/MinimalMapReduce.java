// == MinimalMapReduce 最简单的 MapReduce 驱动程序，使用默认配置
//
// 本示例展示了运行 MapReduce 作业所需的最少代码量。

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv MinimalMapReduce
/**
 * 最简单的 MapReduce 作业示例。
 *
 * <p>该示例展示了如何用最少的代码创建一个 MapReduce 作业。
 * 它完全依赖 Hadoop 的默认配置：
 * <ul>
 *   <li>输入格式：TextInputFormat（按行读取，每行偏移量为键，内容为值）</li>
 *   <li>输出格式：TextOutputFormat（键\t值 格式）</li>
 *   <li>Mapper：默认 IdentityMapper（直接传递键值对）</li>
 *   <li>Reducer：默认 IdentityReducer（直接传递键值对）</li>
 *   <li>分区器：默认 HashPartitioner（按哈希分区）</li>
 *   <li>reduce 任务数：默认 1</li>
 * </ul>
 *
 * <p>用法：
 * <pre>
 * hadoop jar app.jar minimal.MR input output
 * </pre>
 *
 * <p>这个示例适合作为学习 MapReduce 的起点，理解作业的基本结构。
 */
public class MinimalMapReduce extends Configured implements Tool {

    /**
     * 运行 MapReduce 作业。
     *
     * @param args 命令行参数，包含输入路径和输出路径
     * @return 作业成功返回 0，失败返回 -1
     * @throws Exception 如果作业执行过程中发生错误
     */
    @Override
    public int run(String[] args) throws Exception {
        // 检查命令行参数数量
        if (args.length != 2) {
            // 输出用法信息到标准错误流
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            // 打印通用的 Hadoop 命令行选项（如 -D, -files 等）
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        // 创建作业实例，传入配置
        Job job = new Job(getConf());
        // 设置运行作业的主类（用于打包和查找 JAR）
        job.setJarByClass(getClass());

        // 设置输入路径（支持通配符，如 /path/to/input*）
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 设置输出路径（该路径不应存在，否则会报错）
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交作业并等待完成，返回是否成功
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * MapReduce 作业的入口点。
     *
     * @param args 命令行参数 [input, output]
     */
    public static void main(String[] args) throws Exception {
        // 使用 ToolRunner 运行作业，它会处理配置并调用 run 方法
        int exitCode = ToolRunner.run(new MinimalMapReduce(), args);
        // 将退出码传递给 JVM，0 表示成功，非 0 表示失败
        System.exit(exitCode);
    }
}
// ^^ MinimalMapReduce
