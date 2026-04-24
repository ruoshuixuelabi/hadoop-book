// cc AvroSort A MapReduce program to sort an Avro data file
// ^^ AvroSort

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Avro 数据文件排序程序。
 *
 * <p>演示如何对 Avro 数据文件进行排序：
 * <ul>
 *   <li>使用 AvroKeyInputFormat 读取 Avro 数据文件</li>
 *   <li>利用 MapReduce 的默认排序功能对 AvroKey 进行排序</li>
 *   <li>使用 AvroKeyOutputFormat 输出排序后的 Avro 文件</li>
 * </ul>
 *
 * <p>这是一个通用的排序程序，可以对任何 Avro 记录进行排序，
 * 只需提供相应的 schema 文件。排序顺序由 Avro 的字典序决定。
 *
 * <p>用法：
 * <pre>
 * hadoop jar avro-examples.jar AvroSort [input] [output] [schema-file]
 * </pre>
 */
public class AvroSort extends Configured implements Tool {

    /**
     * SortMapper: 将输入的 AvroKey 直接传递到输出。
     *
     * <p>由于 AvroKeyInputFormat 已经将数据读取为 AvroKey，
     * 并且 MapReduce 框架会自动对 AvroKey 进行排序，
     * 因此 Mapper 只需要将键和值原封不动地传递即可。
     *
     * <p>注意：这里使用了泛型 K，允许处理任意类型的 Avro 记录。
     *
     * @param <K> Avro 记录的类型
     */
    static class SortMapper<K> extends Mapper<AvroKey<K>, NullWritable, AvroKey<K>, AvroValue<K>> {
        /**
         * 将输入的 AvroKey 和对应的值 (datum) 传递到下一步。
         *
         * @param key 输入的 AvroKey
         * @param value 始终为 NullWritable，因为 Avro 数据文件中键即数据
         * @param context Mapper 上下文
         */
        @Override
        protected void map(AvroKey<K> key, NullWritable value, Context context) throws IOException, InterruptedException {
            // AvroKey.datum() 获取存储的实际数据
            context.write(key, new AvroValue<K>(key.datum()));
        }
    }

    /**
     * SortReducer: 保持排序顺序输出数据。
     *
     * <p>Reducer 的输入已经按照 AvroKey 排序，
     * Reducer 只需将每个键对应的值列表输出即可。
     * 对于排序场景，每个键通常只有一个值。
     *
     * <p>输出时使用 NullWritable 作为键，因为数据已经包含在 AvroKey 中。
     *
     * @param <K> Avro 记录的类型
     */
    static class SortReducer<K> extends Reducer<AvroKey<K>, AvroValue<K>, AvroKey<K>, NullWritable> {
        /**
         * 遍历值并输出排序后的结果。
         *
         * @param key 已排序的 AvroKey
         * @param values 该键对应的所有值
         * @param context Reducer 上下文
         */
        @Override
        protected void reduce(AvroKey<K> key, Iterable<AvroValue<K>> values, Context context) throws IOException, InterruptedException {
            for (AvroValue<K> value : values) {
                // 输出排序后的数据
                context.write(new AvroKey(value.datum()), NullWritable.get());
            }
        }
    }

    /**
     * 配置并执行 Avro 排序作业。
     *
     * @param args 命令行参数：[input] [output] [schema-file]
     * @return 作业退出码
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf(
                    "Usage: %s [generic options] <input> <output> <schema-file>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        String input = args[0];
        String output = args[1];
        String schemaFile = args[2];

        Job job = new Job(getConf(), "Avro sort");
        job.setJarByClass(getClass());
        // 设置用户类路径优先，避免与集群版本冲突
        job.getConfiguration().setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 设置 GenericData 作为数据模型，支持任意 Avro 类型
        AvroJob.setDataModelClass(job, GenericData.class);

        // 解析 schema 文件
        Schema schema = new Schema.Parser().parse(new File(schemaFile));

        // 设置输入键的 schema (即数据的 schema)
        AvroJob.setInputKeySchema(job, schema);
        // Map 输出键的 schema
        AvroJob.setMapOutputKeySchema(job, schema);
        // Map 输出值的 schema
        AvroJob.setMapOutputValueSchema(job, schema);
        // 最终输出键的 schema
        AvroJob.setOutputKeySchema(job, schema);

        // 使用 Avro 特定的输入输出格式
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroSort(), args);
        System.exit(exitCode);
    }
}
// ^^ AvroSort