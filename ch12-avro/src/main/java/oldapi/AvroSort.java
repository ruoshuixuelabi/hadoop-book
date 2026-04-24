package oldapi;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Avro 数据文件排序程序 (旧 API 版本)。
 *
 * <p>使用 Hadoop 旧 MapReduce API 对 Avro 数据文件进行排序。
 * 演示了旧 API 中 Avro 集成的使用方法。
 *
 * <p>与新版 {@link AvroSort} 的主要区别:
 * <ul>
 *   <li>使用 AvroMapper/AvroReducer 基类</li>
 *   <li>使用 AvroCollector 收集输出</li>
 *   <li>使用 Pair 类封装键值对</li>
 * </ul>
 *
 * <p>用法:
 * <pre>
 * hadoop jar avro-examples.jar oldapi.AvroSort [input] [output] [schema-file]
 * </pre>
 */
public class AvroSort extends Configured implements Tool {

    /**
     * Mapper: 将输入数据转换为 Pair 形式传递。
     *
     * <p>使用 Avro 的 Pair 类同时传递键和值，
     * 这里将原始数据作为键和值的第一个元素。
     *
     * @param <K> 数据类型
     */
    static class SortMapper<K> extends AvroMapper<K, Pair<K, K>> {
        /**
         * 将数据转换为键值对形式。
         *
         * @param datum 输入数据
         * @param collector Pair 收集器
         * @param reporter 进度报告器
         */
        public void map(K datum, AvroCollector<Pair<K, K>> collector,
            Reporter reporter) throws IOException {
            // 创建 (datum, null) 对，保持原数据在对的第一个位置
            collector.collect(new Pair<K, K>(datum, null, datum, null));
        }
    }

    /**
     * Reducer: 保持排序顺序输出数据。
     *
     * <p>由于 MapReduce 会对键进行排序，
     * Reducer 收到的数据已经排好序。
     */
    static class SortReducer<K> extends AvroReducer<K, K, K> {
        /**
         * 按顺序输出所有值。
         */
        public void reduce(K key, Iterable<K> values,
            AvroCollector<K> collector,
            Reporter reporter) throws IOException {
            for (K value : values) {
                collector.collect(value);
            }
        }
    }

    /**
     * 配置并运行排序作业。
     *
     * @param args 命令行参数: [input] [output] [schema-file]
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

        JobConf conf = new JobConf(getConf(), getClass());
        conf.setJobName("Avro sort");

        // 设置 GenericData 数据模型
        AvroJob.setDataModelClass(conf, GenericData.class);

        FileInputFormat.addInputPath(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        // 解析 schema 文件
        Schema schema = new Schema.Parser().parse(new File(schemaFile));
        // 输入 schema
        AvroJob.setInputSchema(conf, schema);
        // 中间 schema: Pair<原始类型, 原始类型>
        Schema intermediateSchema = Pair.getPairSchema(schema, schema);
        AvroJob.setMapOutputSchema(conf, intermediateSchema);
        // 输出 schema: 原始类型
        AvroJob.setOutputSchema(conf, schema);

        AvroJob.setMapperClass(conf, SortMapper.class);
        AvroJob.setReducerClass(conf, SortReducer.class);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroSort(), args);
        System.exit(exitCode);
    }
}