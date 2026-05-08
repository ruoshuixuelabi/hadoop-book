package crunch;

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Tuple3;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.Sort;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import static crunch.PCollections.dump;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Crunch类型系统测试
 *
 * 演示Crunch支持的各种数据类型:
 * - Avro Reflect(通过反射自动映射)
 * - Avro Specific(编译时生成的特定类型)
 * - Writable类型
 * - 多种Avro类型: pairs, triples
 *
 * 测试类型转换和排序功能。
 */
public class TypesTest implements Serializable {

    /** 临时目录规则 */
    @Rule
    public transient TemporaryPath tmpDir = new TemporaryPath();

    /**
     * 测试Avro Reflect类型
     *
     * 使用WeatherRecord类通过反射自动创建Avro schema
     */
    @Test
    public void testAvroReflect() throws Exception {
        String inputPath = tmpDir.copyResourceFileName("sample.txt");
        Pipeline pipeline = new MRPipeline(getClass());

        // 读取文本行
        PCollection<String> lines = pipeline.read(From.textFile(inputPath));

        // 转换为WeatherRecord
        PCollection<WeatherRecord> records = lines.parallelDo(
            new DoFn<String, WeatherRecord>() {
                NcdcRecordParser parser = new NcdcRecordParser();

                @Override
                public void process(String input, Emitter<WeatherRecord> emitter) {
                    parser.parse(input);
                    if (parser.isValidTemperature()) {
                        emitter.emit(new WeatherRecord(parser.getYearInt(),
                            parser.getAirTemperature(), parser.getStationId()));
                    }
                }
            }, Avros.records(WeatherRecord.class));

        // 按(年份, 气温)键创建表
        PTable<Pair<Integer, Integer>, WeatherRecord> table = records.by(
            new MapFn<WeatherRecord, Pair<Integer, Integer>>() {
                @Override
                public Pair<Integer, Integer> map(WeatherRecord input) {
                    return Pair.of(input.getYear(), input.getTemperature());
                }
            }, Avros.pairs(Avros.ints(), Avros.ints()));

        // 按键排序并获取值
        PCollection<WeatherRecord> sortedRecords = Sort.sort(table).values();

        // 物化结果
        Iterable<WeatherRecord> materialized = sortedRecords.materialize();

        // 期望的排序结果(按年份升序)
        List<WeatherRecord> expectedContent = Lists.newArrayList(
            new WeatherRecord(1949, 78, "012650-99999"),
            new WeatherRecord(1949, 111, "012650-99999"),
            new WeatherRecord(1950, -11, "011990-99999"),
            new WeatherRecord(1950, 0, "011990-99999"),
            new WeatherRecord(1950, 22, "011990-99999")
        );
        assertEquals(expectedContent, Lists.newArrayList(materialized));

        pipeline.done();
    }

    /**
     * 测试Avro Reflect二次排序
     *
     * 使用Tuple3实现按多个字段排序
     */
    @Test
    public void testAvroReflectSecondarySort() throws Exception {
        String inputPath = tmpDir.copyResourceFileName("sample.txt");
        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> lines = pipeline.read(From.textFile(inputPath));

        // 转换为WeatherRecord
        PCollection<WeatherRecord> records = lines.parallelDo(
            new DoFn<String, WeatherRecord>() {
                NcdcRecordParser parser = new NcdcRecordParser();

                @Override
                public void process(String input, Emitter<WeatherRecord> emitter) {
                    parser.parse(input);
                    if (parser.isValidTemperature()) {
                        emitter.emit(new WeatherRecord(parser.getYearInt(),
                            parser.getAirTemperature(), parser.getStationId()));
                    }
                }
            }, Avros.records(WeatherRecord.class));

        // 转换为三元组 (年份, 气温, 记录)
        PCollection<Tuple3<Integer, Integer, WeatherRecord>> triples = records.parallelDo(
            new DoFn<WeatherRecord, Tuple3<Integer, Integer, WeatherRecord>>() {
                @Override
                public void process(WeatherRecord input,
                    Emitter<Tuple3<Integer, Integer, WeatherRecord>> emitter) {
                    emitter.emit(Tuple3.of(input.getYear(), input.getTemperature(), input));
                }
            }, Avros.triples(Avros.ints(), Avros.ints(), Avros.records(WeatherRecord.class))
        );

        // 按年份升序、气温降序排序
        PCollection<Tuple3<Integer, Integer, WeatherRecord>> sorted = Sort
            .sortTriples(triples, Sort.ColumnOrder.by(1, Sort.Order.ASCENDING),
                Sort.ColumnOrder.by(2, Sort.Order.DESCENDING));

        // 提取排序后的记录
        PCollection<WeatherRecord> sortedRecords = sorted.parallelDo(new DoFn<Tuple3<Integer,
            Integer, WeatherRecord>, WeatherRecord>() {
            @Override
            public void process(Tuple3<Integer, Integer, WeatherRecord> input,
                Emitter<WeatherRecord> emitter) {
                emitter.emit(input.third());
            }
        }, Avros.records(WeatherRecord.class));

        Iterable<WeatherRecord> materialized = sortedRecords.materialize();

        // 期望的排序结果(年份升序,同年份内气温降序)
        List<WeatherRecord> expectedContent = Lists.newArrayList(
            new WeatherRecord(1949, 111, "012650-99999"), // 1949年最高气温
            new WeatherRecord(1949, 78, "012650-99999"),
            new WeatherRecord(1950, 22, "011990-99999"), // 1950年最高气温
            new WeatherRecord(1950, 0, "011990-99999"),
            new WeatherRecord(1950, -11, "011990-99999")
        );
        assertEquals(expectedContent, Lists.newArrayList(materialized));

        pipeline.done();
    }

    /**
     * 测试Writable类型写入Avro文件会失败
     */
    @Test(expected = CrunchRuntimeException.class)
    public void testWriteWritableToAvroFileFails() throws Exception {
        String inputPath = tmpDir.copyResourceFileName("ints.txt");
        String outputPath = tmpDir.getFileName("out");
        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> a = pipeline.read(From.textFile(inputPath));
        // Writable类型无法序列化为Avro,会抛出异常
        a.write(To.avroFile(outputPath));
        pipeline.done();
    }

    /**
     * 测试Avro类型写入SequenceFile会失败
     */
    @Test
    public void testWriteAvroToSequenceFileFails() throws Exception {
        String inputPath = tmpDir.copyResourceFileName("ints.txt");
        String outputPath = tmpDir.getFileName("out");
        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> a = pipeline.read(From.textFile(inputPath, Avros.strings()));
        // Avro类型无法用于SequenceFile的Key
        a.write(To.sequenceFile(outputPath));
        PipelineResult result = pipeline.done();
        assertFalse(result.succeeded());
    }

    /**
     * 测试Writable到Avro的转换
     */
    @Test
    public void testConvertWritableToAvro() throws Exception {
        String inputPath = tmpDir.copyResourceFileName("ints.txt");
        String outputPath = tmpDir.getFileName("out");
        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> a = pipeline.read(From.textFile(inputPath));
        // 使用IdentityFn转换为Avro字符串类型
        PCollection<String> b = a.parallelDo(IdentityFn.<String>getInstance(), Avros.strings());
        b.write(To.avroFile(outputPath));
        pipeline.done();
    }

    /**
     * 测试写入SequenceFile和从中读取
     */
    @Test
    public void testWriteTextToSequenceFile() throws Exception {
        String inputPath = tmpDir.copyResourceFileName("ints.txt");
        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> lines = pipeline.read(From.textFile(inputPath));
        assertEquals(WritableTypeFamily.getInstance(), lines.getPType().getFamily());

        String outputPath = tmpDir.getFileName("out");
        lines.write(To.sequenceFile(outputPath));
        pipeline.done();

        // 重新读取
        Pipeline pipeline2 = new MRPipeline(getClass());
        PTable<NullWritable, Text> d = pipeline2.read(From.sequenceFile(outputPath,
            NullWritable.class, Text.class));
        PCollection<Text> e = d.values();
        assertEquals("{2,3,1,3}", dump(e));

        pipeline2.done();
    }

}