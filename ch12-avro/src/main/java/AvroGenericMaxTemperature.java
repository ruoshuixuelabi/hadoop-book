// cc AvroGenericMaxTemperature MapReduce program to find the maximum temperature, creating Avro output
// ^^ AvroGenericMaxTemperature

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 使用 Avro Generic API 查找最高温度的 MapReduce 程序。
 *
 * <p>本程序演示了如何使用 Avro 的 GenericRecord API 处理数据：
 * <ul>
 *   <li>Mapper 读取文本格式的 NCDC 天气数据，解析出有效记录</li>
 *   <li>使用 AvroKey/AvroValue 封装键值对，在 MapReduce 中传输 Avro 数据</li>
 *   <li>Reducer 找出每个年份的最高温度</li>
 *   <li>输出使用 AvroKeyOutputFormat 写入 Avro 数据文件</li>
 * </ul>
 *
 * <p>Avro Generic API 使用通用的 Avro 记录结构，不需要编译时生成特定类。
 * 适用于 schema 动态变化或不想使用代码生成的场景。
 *
 * @see AvroSpecificMaxTemperature 使用 Specific API 的对比实现
 */
public class AvroGenericMaxTemperature extends Configured implements Tool {

    /**
     * Avro Schema 定义了天气记录的结构。
     *
     * <p>包含三个字段：
     * <ul>
     *   <li>year: 年份 (int 类型)</li>
     *   <li>temperature: 温度 (int 类型)</li>
     *   <li>stationId: 气象站 ID (string 类型)</li>
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
     * Mapper 任务：解析 NCDC 天气数据，输出年份和天气记录。
     *
     * <p>输入：LongWritable (行偏移量), Text (原始数据行)
     * <p>输出：AvroKey&lt;Integer&gt; (年份), AvroValue&lt;GenericRecord&gt; (天气记录)
     *
     * <p>只输出有效温度记录，过滤掉：
     * <ul>
     *   <li>缺失温度 (9999)</li>
     *   <li>格式错误的温度</li>
     *   <li>质量标识不正确的记录</li>
     * </ul>
     */
    public static class MaxTemperatureMapper
            extends Mapper<LongWritable, Text, AvroKey<Integer>,
            AvroValue<GenericRecord>> {
        private NcdcRecordParser parser = new NcdcRecordParser();
        private GenericRecord record = new GenericData.Record(SCHEMA);

        /**
         * 解析每行数据并提取有效的天气记录。
         *
         * @param key 行偏移量
         * @param value 原始 NCDC 数据行
         * @param context Mapper 上下文，用于输出结果
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            parser.parse(value.toString());
            // 只处理有效温度记录
            if (parser.isValidTemperature()) {
                // 填充 Avro 记录字段
                record.put("year", parser.getYearInt());
                record.put("temperature", parser.getAirTemperature());
                record.put("stationId", parser.getStationId());
                // 按年份分组，年份作为 Map 输出键
                context.write(new AvroKey<Integer>(parser.getYearInt()),
                        new AvroValue<GenericRecord>(record));
            }
        }
    }

    /**
     * Reducer 任务：找出每个年份的最高温度。
     *
     * <p>输入：AvroKey&lt;Integer&gt; (年份), AvroValue&lt;GenericRecord&gt; (该年份所有记录)
     * <p>输出：AvroKey&lt;GenericRecord&gt; (最高温度记录), NullWritable
     */
    public static class MaxTemperatureReducer
            extends Reducer<AvroKey<Integer>, AvroValue<GenericRecord>,
            AvroKey<GenericRecord>, NullWritable> {

        /**
         * 遍历同一年份的所有记录，找出温度最高的那个。
         *
         * @param key 年份
         * @param values 该年份的所有天气记录
         * @param context Reducer 上下文
         */
        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<GenericRecord>>
                values, Context context) throws IOException, InterruptedException {
            GenericRecord max = null;
            for (AvroValue<GenericRecord> value : values) {
                GenericRecord record = value.datum();
                // 比较温度，找出最大值
                if (max == null ||
                        (Integer) record.get("temperature") > (Integer) max.get("temperature")) {
                    max = newWeatherRecord(record);
                }
            }
            // 输出最高温度记录，键为 null (Avro 文件以值为准)
            context.write(new AvroKey(max), NullWritable.get());
        }

        /**
         * 根据现有记录创建一个新的天气记录。
         * 用于保留原始记录数据。
         *
         * @param value 原始记录
         * @return 新的天气记录副本
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
     * 配置并执行最高温度 MapReduce 作业。
     *
     * @param args 命令行参数：[input] [output]
     * @return 作业退出码 (0 成功，非 0 失败)
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job(getConf(), "Max temperature");
        job.setJarByClass(getClass());

        // 设置用户类路径优先，避免与 Hadoop 集群的 Avro 版本冲突
        job.getConfiguration().setBoolean(
                Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置 Avro 数据类型的 Schema
        // Map 输出键为整数类型，用于按年份分组
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        // Map 输出值为完整的天气记录
        AvroJob.setMapOutputValueSchema(job, SCHEMA);
        // 最终输出为天气记录 Schema
        AvroJob.setOutputKeySchema(job, SCHEMA);

        // 使用标准的 TextInputFormat 读取文本输入
        job.setInputFormatClass(TextInputFormat.class);
        // 使用 AvroKeyOutputFormat 输出 Avro 数据文件
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroGenericMaxTemperature(), args);
        System.exit(exitCode);
    }
}
// ^^ AvroGenericMaxTemperature