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
 * 将文本文件转换为 Parquet 文件，使用 Parquet 的 ExampleOutputFormat。
 *
 * <p>本程序演示如何使用 Parquet 的原生 API (Example) 来写入 Parquet 文件：
 * <ul>
 *   <li>使用标准 TextInputFormat 读取文本输入</li>
 *   <li>使用 ExampleOutputFormat 输出 Parquet 文件</li>
 *   <li>使用 Parquet 的 MessageType 定义 schema</li>
 *   <li>使用 Group 和 GroupFactory 创建记录</li>
 * </ul>
 *
 * <p>与 {@link TextToParquetWithAvro} 的对比：
 * <ul>
 *   <li>使用 Parquet 的原生 API 而非 Avro 集成</li>
 *   <li>Schema 使用 Parquet 的 MessageType 格式定义</li>
 *   <li>适合不需要 Avro 特定功能的场景</li>
 * </ul>
 *
 * <p>用法：
 * <pre>
 * hadoop jar parquet-examples.jar TextToParquetWithExample [input] [output]
 * </pre>
 */
public class TextToParquetWithExample extends Configured implements Tool {

    /**
     * Parquet MessageType Schema 定义。
     *
     * <p>Parquet 使用自己的 schema 语言，与 Avro 不同：
     * <ul>
     *   <li>message 相当于 Avro 的 record</li>
     *   <li>required 表示必填字段（非 null）</li>
     *   <li>binary (UTF8) 对应 Avro 的 string</li>
     *   <li>int64 对应 Avro 的 long</li>
     * </ul>
     */
    private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
        "message Line {\n" +
        "  required int64 offset;\n" +
        "  required binary line (UTF8);\n" +
        "}");

    /**
     * Mapper: 读取文本行，输出 Parquet Group。
     *
     * <p>输入: LongWritable (偏移量), Text (行内容)
     * <p>输出: Void (键), Group (Parquet 记录)
     *
     * <p>Group 是 Parquet 的基本数据类型，类似于 Avro 的 GenericRecord。
     */
    public static class TextToParquetMapper
        extends Mapper<LongWritable, Text, Void, Group> {

        // 使用 SimpleGroupFactory 创建 Group 实例
        private GroupFactory groupFactory = new SimpleGroupFactory(SCHEMA);

        /**
         * 将每行文本转换为 Parquet Group 并输出。
         *
         * @param key 行在文件中的偏移量
         * @param value 行的文本内容
         * @param context Mapper 上下文
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            // 创建新的 Group 并填充字段
            Group group = groupFactory.newGroup()
                .append("offset", key.get())
                .append("line", value.toString());
            // 输出到 Parquet 文件
            context.write(null, group);
        }
    }

    /**
     * 配置并运行文本到 Parquet 的转换作业。
     *
     * @param args 命令行参数：[input] [output]
     * @return 作业退出码
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job(getConf(), "Text to Parquet");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TextToParquetMapper.class);
        // 禁用 Reduce 阶段
        job.setNumReduceTasks(0);

        // 设置输出格式为 ExampleOutputFormat
        job.setOutputFormatClass(ExampleOutputFormat.class);
        // 设置 Parquet schema
        ExampleOutputFormat.setSchema(job, SCHEMA);

        // 设置输出键值类型
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TextToParquetWithExample(), args);
        System.exit(exitCode);
    }
}