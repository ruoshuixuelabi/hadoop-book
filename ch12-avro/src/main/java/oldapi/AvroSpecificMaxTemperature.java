package oldapi;

import java.io.IOException;
import org.apache.avro.Schema;
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
import specific.WeatherRecord;

/**
 * 使用 Avro Specific API 查找最高温度的 MapReduce 程序 (旧 API 版本)。
 *
 * <p>本程序使用 Hadoop 旧 MapReduce API 与 Avro Specific 记录类型:
 * <ul>
 *   <li>使用 AvroUtf8InputFormat 读取输入</li>
 *   <li>使用编译时生成的 WeatherRecord 特定类 (代码生成)</li>
 *   <li>使用 AvroReducer 的 Combiner 优化本地聚合</li>
 * </ul>
 *
 * <p>与 {@link oldapi.AvroGenericMaxTemperature} 对比:
 * <ul>
 *   <li>使用 WeatherRecord 特定类而非 GenericRecord</li>
 *   <li>性能更好 (无需运行时反射)</li>
 *   <li>需要在编译时通过 avro-maven-plugin 生成 WeatherRecord 类</li>
 * </ul>
 *
 * @see oldapi.AvroGenericMaxTemperature 使用 Generic API 的实现
 */
public class AvroSpecificMaxTemperature extends Configured implements Tool {

    /**
     * Mapper: 解析文本并输出年份-WeatherRecord 对。
     *
     * <p>使用 WeatherRecord 的 setter 方法设置字段值，
     * 这比 GenericRecord 的 put() 方法更类型安全。
     */
    public static class MaxTemperatureMapper
        extends AvroMapper<Utf8, Pair<Integer, WeatherRecord>> {
        private NcdcRecordParser parser = new NcdcRecordParser();
        private WeatherRecord record = new WeatherRecord();

        /**
         * 解析文本行并输出有效的天气记录。
         */
        @Override
        public void map(Utf8 line,
            AvroCollector<Pair<Integer, WeatherRecord>> collector,
            Reporter reporter) throws IOException {
            parser.parse(line.toString());
            if (parser.isValidTemperature()) {
                record.setYear(parser.getYearInt());
                record.setTemperature(parser.getAirTemperature());
                record.setStationId(parser.getStationId());
                collector.collect(
                    new Pair<Integer, WeatherRecord>(parser.getYearInt(), record));
            }
        }
    }

    /**
     * Reducer: 找出每个年份的最高温度。
     */
    public static class MaxTemperatureReducer extends
        AvroReducer<Integer, WeatherRecord, WeatherRecord> {

        /**
         * 遍历同一年份的所有记录，返回温度最高的。
         */
        @Override
        public void reduce(Integer key, Iterable<WeatherRecord> values,
            AvroCollector<WeatherRecord> collector,
            Reporter reporter) throws IOException {
            WeatherRecord max = null;
            for (WeatherRecord value : values) {
                if (max == null || value.getTemperature() > max.getTemperature()) {
                    max = newWeatherRecord(value);
                }
            }
            collector.collect(max);
        }
    }

    /**
     * Combiner: 在 Mapper 本地先做一次聚合，优化网络传输。
     *
     * <p>与 Reducer 做同样的工作，但只在本地节点执行。
     * 这可以减少 Shuffle 阶段传输的数据量。
     */
    public static class MaxTemperatureCombiner extends
        AvroReducer<Integer, WeatherRecord, Pair<Integer, WeatherRecord>> {

        @Override
        public void reduce(Integer key, Iterable<WeatherRecord> values,
            AvroCollector<Pair<Integer, WeatherRecord>> collector,
            Reporter reporter) throws IOException {
            WeatherRecord max = null;
            for (WeatherRecord value : values) {
                if (max == null || value.getTemperature() > max.getTemperature()) {
                    max = newWeatherRecord(value);
                }
            }
            collector.collect(new Pair<Integer, WeatherRecord>(key, max));
        }
    }

    /**
     * 创建 WeatherRecord 副本。
     */
    private static WeatherRecord newWeatherRecord(WeatherRecord value) {
        return new WeatherRecord(value.getYear(), value.getTemperature(), value.getStationId());
    }

    /**
     * 配置并运行最高温度作业。
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

        // 输入为字符串类型
        AvroJob.setInputSchema(conf, Schema.create(Schema.Type.STRING));
        // Map 输出为年份-天气记录对
        AvroJob.setMapOutputSchema(conf, Pair.getPairSchema(
            Schema.create(Schema.Type.INT), WeatherRecord.SCHEMA$));
        // 最终输出为天气记录
        AvroJob.setOutputSchema(conf, WeatherRecord.SCHEMA$);

        conf.setInputFormat(AvroUtf8InputFormat.class);

        AvroJob.setMapperClass(conf, MaxTemperatureMapper.class);
        // 设置 Combiner 类，在本地先聚合温度数据
        AvroJob.setCombinerClass(conf, MaxTemperatureCombiner.class);
        AvroJob.setReducerClass(conf, MaxTemperatureReducer.class);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroSpecificMaxTemperature(), args);
        System.exit(exitCode);
    }
}