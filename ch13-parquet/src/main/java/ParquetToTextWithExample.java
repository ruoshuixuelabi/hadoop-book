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
 * 将 Parquet 文件转换为文本文件，使用 Parquet 的 ExampleInputFormat。
 *
 * <p>本程序演示如何使用 Parquet 的原生 API 来读取 Parquet 文件：
 * <ul>
 *   <li>使用 ExampleInputFormat 读取 Parquet 文件</li>
 *   <li>输出为普通的文本格式</li>
 *   <li>每行输出 Parquet Group 的字符串表示</li>
 * </ul>
 *
 * <p>与 {@link ParquetToTextWithAvro} 的对比：
 * <ul>
 *   <li>使用 ExampleInputFormat 而非 AvroParquetInputFormat</li>
 *   <li>返回 Parquet 的 Group 对象而非 IndexedRecord</li>
 *   <li>适合不需要 Avro Schema 的场景</li>
 * </ul>
 *
 * <p>用法：
 * <pre>
 * hadoop jar parquet-examples.jar ParquetToTextWithExample [input] [output]
 * </pre>
 */
public class ParquetToTextWithExample extends Configured implements Tool {

    /**
     * Mapper: 读取 Parquet Group，输出文本。
     *
     * <p>输入: Void (Parquet 文件自带键), Group (Parquet 记录)
     * <p>输出: NullWritable (键), Text (Group 的字符串表示)
     *
     * <p>Group 是 Parquet 的基本数据类型，类似于 Avro 的 GenericRecord，
     * 可以通过字段名或索引访问字段值。
     */
    public static class ParquetToTextMapper
        extends Mapper<Void, Group, NullWritable, Text> {

        /**
         * 将 Parquet Group 转换为文本行。
         *
         * @param key Parquet 记录的键（忽略）
         * @param value Parquet Group
         * @param context Mapper 上下文
         */
        @Override
        protected void map(Void key, Group value, Context context)
            throws IOException, InterruptedException {
            // Group.toString() 输出字段的字符串表示
            context.write(NullWritable.get(), new Text(value.toString()));
        }
    }

    /**
     * 配置并运行 Parquet 到文本的转换作业。
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

        Job job = new Job(getConf(), "Parquet to text");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ParquetToTextMapper.class);
        // 禁用 Reduce 阶段
        job.setNumReduceTasks(0);

        // 使用 ExampleInputFormat 读取 Parquet 文件
        job.setInputFormatClass(ExampleInputFormat.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ParquetToTextWithExample(), args);
        System.exit(exitCode);
    }
}