package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 最简单的 MapReduce 作业示例（旧 API）。
 *
 * <p>本示例使用 Hadoop 旧 API（mapred 包）演示最简化的 MapReduce 作业。
 * 与新 API（mapreduce 包）相比，旧 API 的配置方式略有不同。
 *
 * <p>该示例完全依赖默认配置：
 * <ul>
 *   <li>输入格式：TextInputFormat</li>
 *   <li>输出格式：TextOutputFormat</li>
 *   <li>Mapper：IdentityMapper</li>
 *   <li>Reducer：IdentityReducer</li>
 * </ul>
 *
 * <p>用法：
 * <pre>
 * hadoop jar app.jar oldapi.MinimalMapReduce input output
 * </pre>
 *
 * @see org.apache.hadoop.mapred.lib.IdentityMapper
 * @see org.apache.hadoop.mapred.lib.IdentityReducer
 */
public class MinimalMapReduce extends Configured implements Tool {

    /**
     * 运行 MapReduce 作业。
     *
     * @param args 命令行参数 [input, output]
     * @return 作业成功返回 0，失败返回 -1
     * @throws Exception 如果作业执行过程中发生错误
     */
    @Override
    public int run(String[] args) throws Exception {
        // 检查命令行参数数量
        if (args.length != 2) {
            // 输出用法信息
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                getClass().getSimpleName());
            // 打印通用命令行选项
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        // 创建 JobConf（旧 API 的作业配置类）
        JobConf conf = new JobConf(getConf(), getClass());
        // 设置输入路径
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        // 设置输出路径
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // 运行作业
        JobClient.runJob(conf);
        return 0;
    }

    /**
     * 程序入口点。
     *
     * @param args 命令行参数
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MinimalMapReduce(), args);
        System.exit(exitCode);
    }
}
