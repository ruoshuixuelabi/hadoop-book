import java.io.IOException;
import org.apache.avro.generic.IndexedRecord;
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
import org.apache.parquet.avro.AvroParquetInputFormat;

/**
 * 将 Parquet 文件转换为文本文件，使用 AvroParquetInputFormat。
 *
 * <p>本程序演示如何使用 Parquet 的 Avro 集成来读取 Parquet 文件：
 * <ul>
 *   <li>使用 AvroParquetInputFormat 读取 Parquet 文件</li>
 *   <li>输出为普通的文本格式</li>
 *   <li>每行输出一个 JSON 格式的 Avro 记录</li>
 * </ul>
 *
 * <p>与 {@link ParquetToTextWithExample} 的对比：
 * <ul>
 *   <li>AvroParquetInputFormat 使用 Avro 的 IndexedRecord 接口</li>
 *   <li>可以与 Avro Schema 集成，支持 Schema 投影</li>
 *   <li>输出为 Avro 对象的字符串表示</li>
 * </ul>
 *
 * <p>用法：
 * <pre>
 * hadoop jar parquet-examples.jar ParquetToTextWithAvro [input] [output]
 * </pre>
 */
public class ParquetToTextWithAvro extends Configured implements Tool {

    /**
     * Mapper: 读取 Parquet 记录，输出文本。
     *
     * <p>输入: Void (Parquet 文件自带键), IndexedRecord (Avro 记录)
     * <p>输出: NullWritable (键), Text (Avro 记录的字符串表示)
     *
     * <p>Avro 的 IndexedRecord 接口允许按索引访问字段，
     * 这对于 Parquet 的列式存储非常高效。
     */
    public static class ParquetToTextMapper
        extends Mapper<Void, IndexedRecord, NullWritable, Text> {

        /**
         * 将 Avro 记录转换为文本行。
         *
         * @param key Parquet 记录的键（忽略）
         * @param value Avro IndexedRecord
         * @param context Mapper 上下文
         */
        @Override
        protected void map(Void key, IndexedRecord value, Context context)
            throws IOException, InterruptedException {
            // IndexedRecord.toString() 输出 JSON 格式
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

        // 设置 Mapper 类
        job.setMapperClass(ParquetToTextMapper.class);
        // 禁用 Reduce 阶段
        job.setNumReduceTasks(0);

        // 使用 AvroParquetInputFormat 读取 Parquet 文件
        job.setInputFormatClass(AvroParquetInputFormat.class);

        // 设置输出键值类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ParquetToTextWithAvro(), args);
        System.exit(exitCode);
    }
}