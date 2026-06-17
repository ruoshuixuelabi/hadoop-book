package oldapi;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroUtf8InputFormat;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
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
 * 使用 Avro Generic API 查找最高温度的 MapReduce 程序 (旧 API 版本)。
 *
 * <p>本程序使用 Hadoop 旧 MapReduce API (mapred 包)，
 * 演示了与新 API 不同的编程方式：
 * <ul>
 *   <li>使用 AvroUtf8InputFormat 读取 Avro 数据文件</li>
 *   <li>使用 AvroCollector 收集输出</li>
 *   <li>使用 AvroMapper/AvroReducer 基类</li>
 *   <li>使用 Pair 类封装键值对</li>
 * </ul>
 *
 * <p>与 {@link AvroSpecificMaxTemperature} 对比：
 * <ul>
 *   <li>使用 GenericRecord 而非特定的 WeatherRecord 类</li>
 *   <li>更灵活但性能略低 (需要运行时反射)</li>
 * </ul>
 *
 * @see AvroSpecificMaxTemperature 使用 Specific API 的实现
 */
public class AvroGenericMaxTemperature extends Configured implements Tool {

    /**
     * 天气记录的 Avro Schema 定义。
     *
     * <p>包含三个字段：
     * <ul>
     *   <li>year: 年份 (int)</li>
     *   <li>temperature: 温度 (int)</li>
     *   <li>stationId: 气象站 ID (string)</li>
     * </ul>
     */
    private static final Schema SCHEMA = new Schema.Parser().parse(
        "{" +
        "  \"type\": \"record\"," +
        "  \"name\": \"WeatherRecord\"," +
        "  \"doc\": \"A weather reading.\"," +
        "  \"fields\": [" +
        "    {\"name\": \"year\", \"type\": \"int\"}," +
        "    {\"name\": \"temperature\", \"type\": \"int\"}," +
        "    {\"name\": \"stationId\", \"type\": \"string\"}" +
        "  ]" +
        "}"
    );

    /**
     * Mapper: 解析天气数据，输出年份-记录对。
     *
     * <p>输入: Utf8 格式的文本行
     * <p>输出: Pair&lt;Integer, GenericRecord&gt; (年份, 天气记录)
     */
    public static class MaxTemperatureMapper
        extends AvroMapper<Utf8, Pair<Integer, GenericRecord>> {
        private NcdcRecordParser parser = new NcdcRecordParser();
        private GenericRecord record = new GenericData.Record(SCHEMA);

        /**
         * 解析每行文本并收集有效的天气记录。
         *
         * @param line 输入的文本行
         * @param collector 用于输出 Pair 键值对
         * @param reporter 进度报告器
         */
        @Override
        public void map(Utf8 line,
            AvroCollector<Pair<Integer, GenericRecord>> collector,
            Reporter reporter) throws IOException {
            parser.parse(line.toString());
            if (parser.isValidTemperature()) {
                record.put("year", parser.getYearInt());
                record.put("temperature", parser.getAirTemperature());
                record.put("stationId", parser.getStationId());
                // 输出年份和记录对，年份用于分区和排序
                collector.collect(
                    new Pair<Integer, GenericRecord>(parser.getYearInt(), record));
            }
        }
    }

    /**
     * Reducer: 找出每个年份的最高温度记录。
     *
     * <p>输入: Integer (年份), Iterable&lt;GenericRecord&gt; (该年份所有记录)
     * <p>输出: GenericRecord (最高温度记录)
     */
    public static class MaxTemperatureReducer
        extends AvroReducer<Integer, GenericRecord, GenericRecord> {

        /**
         * 遍历同一年份的所有记录，找出温度最高的。
         *
         * @param key 年份
         * @param values 该年份的所有天气记录
         * @param collector 输出收集器
         * @param reporter 进度报告器
         */
        @Override
        public void reduce(Integer key, Iterable<GenericRecord> values,
            AvroCollector<GenericRecord> collector, Reporter reporter)
            throws IOException {
            GenericRecord max = null;
            for (GenericRecord value : values) {
                if (max == null ||
                    (Integer) value.get("temperature") > (Integer) max.get("temperature")) {
                    max = newWeatherRecord(value);
                }
            }
            collector.collect(max);
        }

        /**
         * 创建新的天气记录副本。
         */
        private GenericRecord newWeatherRecord(GenericRecord value) {
            GenericRecord record = new GenericData.Record(SCHEMA);
            record.put("year", value.get("year"));
            record.put("temperature", value.get("temperature"));
            record.put("stationId", value.get("stationId"));
            return record;
        }
    }

    /**
     * 配置并运行最高温度 MapReduce 作业。
     *
     * @param args 命令行参数: [input] [output]
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

        JobConf conf = new JobConf(getConf(), getClass());
        conf.setJobName("Max temperature");

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // 设置 Avro 输入 schema: 字符串类型 (UTF-8)
        AvroJob.setInputSchema(conf, Schema.create(Schema.Type.STRING));
        // Map 输出 schema: Pair<Integer, GenericRecord>
        AvroJob.setMapOutputSchema(conf,
            Pair.getPairSchema(Schema.create(Schema.Type.INT), SCHEMA));
        // 最终输出 schema: 天气记录
        AvroJob.setOutputSchema(conf, SCHEMA);

        // 使用 AvroUtf8InputFormat 读取 Avro 文件中的 UTF-8 字符串
        conf.setInputFormat(AvroUtf8InputFormat.class);

        AvroJob.setMapperClass(conf, MaxTemperatureMapper.class);
        AvroJob.setReducerClass(conf, MaxTemperatureReducer.class);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroGenericMaxTemperature(), args);
        System.exit(exitCode);
    }
}