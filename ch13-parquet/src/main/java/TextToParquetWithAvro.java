import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
import parquet.avro.AvroParquetOutputFormat;
import parquet.example.data.Group;

/**
 * 将文本文件转换为 Parquet 文件，使用 AvroParquetOutputFormat。
 *
 * <p>本程序演示如何使用 Parquet 的 Avro 集成来写入 Parquet 文件：
 * <ul>
 *   <li>使用标准 TextInputFormat 读取文本输入</li>
 *   <li>使用 AvroParquetOutputFormat 输出 Parquet 文件</li>
 *   <li>Avro Schema 定义了输出数据的结构</li>
 *   <li>每条记录包含 offset（行偏移量）和 line（行内容）</li>
 * </ul>
 *
 * <p>AvroParquetOutputFormat 的优势：
 * <ul>
 *   <li>可以使用 Avro Schema 定义数据类型</li>
 *   <li>支持 GenericRecord 和 SpecificRecord</li>
 *   <li>列式存储提供高效的压缩和查询性能</li>
 * </ul>
 *
 * <p>用法：
 * <pre>
 * hadoop jar parquet-examples.jar TextToParquetWithAvro [input] [output]
 * </pre>
 */
public class TextToParquetWithAvro extends Configured implements Tool {

    /**
     * Avro Schema 定义：每条记录包含 offset 和 line 两个字段。
     *
     * <p>offset: 行在文件中的字节偏移量 (long)
     * <p>line: 行的文本内容 (string)
     */
    private static final Schema SCHEMA = new Schema.Parser().parse(
        "{\n" +
        "  \"type\": \"record\"," +
        "  \"name\": \"Line\"," +
        "  \"fields\": [" +
        "    {\"name\": \"offset\", \"type\": \"long\"}," +
        "    {\"name\": \"line\", \"type\": \"string\"}" +
        "  ]" +
        "}");

    /**
     * Mapper: 读取文本行，输出 Avro GenericRecord。
     *
     * <p>输入: LongWritable (偏移量), Text (行内容)
     * <p>输出: Void (键), GenericRecord (Avro 记录)
     *
     * <p>由于不需要 Reduce 阶段，直接将数据写入 Parquet。
     */
    public static class TextToParquetMapper
        extends Mapper<LongWritable, Text, Void, GenericRecord> {

        // 重用 GenericRecord 对象以减少内存分配
        private GenericRecord record = new GenericData.Record(SCHEMA);

        /**
         * 将每行文本转换为 Avro 记录并输出。
         *
         * @param key 行在文件中的偏移量
         * @param value 行的文本内容
         * @param context Mapper 上下文
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            // 设置记录字段
            record.put("offset", key.get());
            record.put("line", value.toString());
            // 输出到 Parquet 文件，键为 null（Parquet 以值为准）
            context.write(null, record);
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

        // 设置输入路径和输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置 Mapper 类
        job.setMapperClass(TextToParquetMapper.class);
        // 禁用 Reduce 阶段（只有 Mapper 直接输出）
        job.setNumReduceTasks(0);

        // 设置输出格式为 AvroParquetOutputFormat
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        // 设置 Avro Schema
        AvroParquetOutputFormat.setSchema(job, SCHEMA);

        // 设置输出键值类型（Parquet 使用 Group 作为值）
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TextToParquetWithAvro(), args);
        System.exit(exitCode);
    }
}