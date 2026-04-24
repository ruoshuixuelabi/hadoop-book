package crunch;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;

import java.io.Serializable;
import java.util.Collections;
import org.apache.crunch.io.To;

import static org.apache.crunch.types.avro.Avros.*;

// Crunch version of ch12-avro AvroGenericMaxTemperature
// Crunch版本的最大气温计算程序,使用Avro通用类型

/**
 * 最大气温计算程序(Avro通用类型)
 *
 * 演示Crunch中如何使用Avro通用类型:
 * - 定义Avro Schema
 * - 使用GenericData.Record创建记录
 * - 自定义Aggregator实现最大值计算
 *
 * Avro通用类型 vs 特定类型:
 * - 通用类型: 使用GenericData.Record,无需编译Schema生成类
 * - 特定类型: 需要用avro-maven-plugin生成特定类,类型安全
 *
 * WeatherRecord Schema:
 * {
 *   "type": "record",
 *   "name": "WeatherRecord",
 *   "fields": [
 *     {"name": "year", "type": "int"},
 *     {"name": "temperature", "type": "int"},
 *     {"name": "stationId", "type": "string"}
 *   ]
 * }
 */
public class AvroGenericMaxTemperatureCrunch implements Serializable {

    /** Avro Schema定义 */
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
     * 主程序入口
     *
     * @param args args[0]: 输入路径
     *             args[1]: Avro文件输出路径
     */
    public static void main(String[] args) throws Exception {
        // 参数验证
        if (args.length != 2) {
            System.err.println("Usage: AvroGenericMaxTemperatureCrunch <input> <output>");
            System.exit(-1);
        }

        // 创建Pipeline
        Pipeline pipeline = new MRPipeline(AvroGenericMaxTemperatureCrunch.class);
        PCollection<String> records = pipeline.readTextFile(args[0]);

        // 转换为(年份, WeatherRecord)对并按年份分组
        PTable<Integer, GenericData.Record> maxTemps = records
          .parallelDo(toYearRecordPairsFn(), tableOf(ints(), generics(SCHEMA)))
          .groupByKey()
          // 使用自定义Aggregator计算每年最高气温记录
          .combineValues(new Aggregators.SimpleAggregator<GenericData.Record>() {
            transient GenericData.Record max;

            @Override
            public void reset() {
                max = null;
            }

            @Override
            public void update(GenericData.Record value) {
                // 比较气温,保留最高气温的记录
                if (max == null ||
                    (Integer) value.get("temperature") > (Integer) max.get("temperature")) {
                    max = newWeatherRecord(value);
                }
            }

            /**
             * 创建新的WeatherRecord
             *
             * @param value 源记录
             * @return 复制的新记录
             */
            private GenericData.Record newWeatherRecord(GenericData.Record value) {
                GenericData.Record record = new GenericData.Record(SCHEMA);
                record.put("year", value.get("year"));
                record.put("temperature", value.get("temperature"));
                record.put("stationId", value.get("stationId"));
                return record;
            }

            @Override
            public Iterable<GenericData.Record> results() {
                return Collections.singleton(max);
            }
          });

        // 写入Avro文件
        pipeline.write(maxTemps, To.avroFile(args[1]));
        pipeline.run();
    }

    /**
     * 将NCDC记录转换为(年份, WeatherRecord)对
     *
     * @return DoFn函数对象
     */
    private static DoFn<String, Pair<Integer, GenericData.Record>> toYearRecordPairsFn() {
        return new DoFn<String, Pair<Integer, GenericData.Record>>() {
            private NcdcRecordParser parser = new NcdcRecordParser();
            private transient GenericData.Record record;

            @Override
            public void process(String input, Emitter<Pair<Integer, GenericData.Record>> emitter) {
                parser.parse(input.toString());
                if (parser.isValidTemperature()) {
                    if (record == null) {
                        record = new GenericData.Record(SCHEMA);
                    }
                    record.put("year", parser.getYearInt());
                    record.put("temperature", parser.getAirTemperature());
                    record.put("stationId", parser.getStationId());
                    emitter.emit(Pair.of(parser.getYearInt(), record));
                }
            }
        };
    }

}