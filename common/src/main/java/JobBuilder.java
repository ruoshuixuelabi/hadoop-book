// == JobBuilder

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * MapReduce 作业构建辅助类（新 API / Hadoop 2.x）
 *
 * 功能说明：
 * - 简化 MapReduce 作业的创建和配置过程
 * - 支持解析标准命令行参数（输入路径、输出路径等）
 * - 提供 fluent 风格的作业构建接口
 *
 * 使用方式：
 * <pre>
 * Job job = new JobBuilder(MyDriver.class)
 *     .withCommandLineArgs(args)
 *     .build();
 * </pre>
 *
 * 支持的命令行参数：
 * - 标准 Hadoop generic options（如 -D property=value）
 * - -overwrite：覆盖输出目录
 * - 输入路径和输出路径（必需）
 * - 额外的自定义参数
 *
 * @since 4.0
 */
public class JobBuilder {
    /** 驱动类（用于设置 JAR 包和作业名称） */
    private final Class<?> driverClass;

    /** MapReduce 作业实例 */
    private final Job job;

    /** 额外参数的数量（用于验证命令行参数个数） */
    private final int extraArgCount;

    /** 额外参数的用法说明字符串 */
    private final String extrArgsUsage;

    /** 解析后的额外参数数组 */
    private String[] extraArgs;

    /**
     * 构造函数：创建 JobBuilder 实例
     *
     * @param driverClass 驱动类（包含 main 方法的类）
     * @throws IOException 如果作业初始化失败
     */
    public JobBuilder(Class<?> driverClass) throws IOException {
        this(driverClass, 0, "");
    }

    /**
     * 构造函数：创建带有额外参数配置的 JobBuilder 实例
     *
     * @param driverClass 驱动类
     * @param extraArgCount 期望的额外参数数量
     * @param extrArgsUsage 额外参数的用法说明
     * @throws IOException 如果作业初始化失败
     */
    public JobBuilder(Class<?> driverClass, int extraArgCount, String extrArgsUsage) throws IOException {
        this.driverClass = driverClass;
        this.extraArgCount = extraArgCount;
        // 创建作业实例并设置 JAR 包类
        this.job = new Job();
        this.job.setJarByClass(driverClass);
        this.extrArgsUsage = extrArgsUsage;
    }

    // vv JobBuilder

    /**
     * 解析输入路径和输出路径参数
     *
     * 静态便捷方法，用于快速创建仅包含输入/输出路径的作业。
     *
     * @param tool Tool 接口实现（通常为驱动类）
     * @param conf Hadoop 配置对象
     * @param args 命令行参数数组（应包含输入路径和输出路径）
     * @return 配置好的 Job 对象，如果参数个数不正确则返回 null
     * @throws IOException 如果创建作业失败
     */
    public static Job parseInputAndOutput(Tool tool, Configuration conf, String[] args) throws IOException {
        // 验证参数个数：必须恰好 2 个（输入路径和输出路径）
        if (args.length != 2) {
            printUsage(tool, "<input> <output>");
            return null;
        }
        // 创建作业并设置 JAR 类和路径
        Job job = new Job(conf);
        job.setJarByClass(tool.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job;
    }

    /**
     * 打印作业用法说明到标准错误流
     *
     * @param tool Tool 接口实现
     * @param extraArgsUsage 额外参数的用法说明
     */
    public static void printUsage(Tool tool, String extraArgsUsage) {
        // 打印程序名称和用法
        System.err.printf("Usage: %s [genericOptions] %s\n\n", tool.getClass().getSimpleName(), extraArgsUsage);
        // 打印 Hadoop 通用选项帮助信息
        GenericOptionsParser.printGenericCommandUsage(System.err);
    }
    // ^^ JobBuilder

    /**
     * 解析命令行参数并配置作业
     *
     * 支持的参数格式：
     * - genericOptions：Hadoop 通用选项（如 -D property=value）
     * - -overwrite：覆盖输出目录（可选）
     * - 输入路径：HDFS 或本地文件系统路径
     * - 输出路径：HDFS 或本地文件系统路径
     * - 额外参数：自定义参数（数量由 extraArgCount 指定）
     *
     * @param args 命令行参数
     * @return this（支持 fluent 风格链式调用）
     * @throws IOException 如果文件系统操作失败
     */
    public JobBuilder withCommandLineArgs(String... args) throws IOException {
        Configuration conf = job.getConfiguration();
        // 解析 generic options
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        // 获取解析后剩余的参数
        String[] otherArgs = parser.getRemainingArgs();

        // 验证参数个数：至少 2 个（输入、输出），最多 3 + extraArgCount 个
        if (otherArgs.length < 2 && otherArgs.length > 3 + extraArgCount) {
            System.err.printf("Usage: %s [genericOptions] [-overwrite] <input path> <output path> %s\n\n",
                    driverClass.getSimpleName(), extrArgsUsage);
            GenericOptionsParser.printGenericCommandUsage(System.err);
            System.exit(-1);
        }

        // 解析可选的 -overwrite 标志和路径参数
        int index = 0;
        boolean overwrite = false;
        if (otherArgs[index].equals("-overwrite")) {
            overwrite = true;
            index++;
        }
        // 解析输入路径
        Path input = new Path(otherArgs[index++]);
        // 解析输出路径
        Path output = new Path(otherArgs[index++]);

        // 收集额外的自定义参数（如果有）
        if (index < otherArgs.length) {
            extraArgs = new String[otherArgs.length - index];
            System.arraycopy(otherArgs, index, extraArgs, 0, otherArgs.length - index);
        }

        // 如果指定了 -overwrite，删除已存在的输出目录
        if (overwrite) {
            output.getFileSystem(conf).delete(output, true);
        }

        // 设置作业的输入/输出路径
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return this;
    }

    /**
     * 构建并返回配置的作业对象
     *
     * @return 配置完成的 Job 对象，可以提交到集群执行
     */
    public Job build() {
        return job;
    }

    /**
     * 获取解析后的额外参数
     *
     * @return 额外参数数组，如果没有额外参数则返回 null
     */
    public String[] getExtraArgs() {
        return extraArgs;
    }
}
