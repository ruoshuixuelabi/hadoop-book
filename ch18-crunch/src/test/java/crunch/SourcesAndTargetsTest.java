package crunch;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.TableSource;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 数据源和目标测试
 *
 * 演示Crunch支持的各种数据源和目标:
 * - 文本文件(textFile)
 * - SequenceFile
 * - Avro文件
 * - 不同的PTypeFamily(Writable vs Avro)
 *
 * 测试数据读写和各种格式转换。
 */
public class SourcesAndTargetsTest implements Serializable {

    /** 临时目录规则 */
    public transient TemporaryPath tmpDir;

    /**
     * 测试readTextFile
     */
    @Test
    public void testReadTextFile() throws IOException {
        List<String> expectedContent = Lists.newArrayList("2", "3", "1", "3");
        String inputPath = tmpDir.copyResourceFileName("ints.txt");
        Pipeline pipeline = MemPipeline.getInstance();
        PCollection<String> lines = pipeline.readTextFile(inputPath);
        Iterable<String> materialized = lines.materialize();
        assertEquals(expectedContent, Lists.newArrayList(materialized));
        pipeline.done();
    }

    /**
     * 测试From.textFile
     */
    @Test
    public void testReadFromTextFile() throws IOException {
        List<String> expectedContent = Lists.newArrayList("2", "3", "1", "3");
        String inputPath = tmpDir.copyResourceFileName("ints.txt");
        Pipeline pipeline = MemPipeline.getInstance();
        PCollection<String> lines = pipeline.read(From.textFile(inputPath));
        Iterable<String> materialized = lines.materialize();
        assertEquals(expectedContent, Lists.newArrayList(materialized));
        pipeline.done();
    }

    /**
     * 测试从文本文件读取为Avro类型
     */
    @Test
    public void testReadFromTextFileAsAvro() throws IOException {
        List<String> expectedContent = Lists.newArrayList("2", "3", "1", "3");
        String inputPath = tmpDir.copyResourceFileName("ints.txt");
        Pipeline pipeline = MemPipeline.getInstance();
        PCollection<String> lines = pipeline.read(From.textFile(inputPath, Avros.strings()));
        Iterable<String> materialized = lines.materialize();
        assertEquals(expectedContent, Lists.newArrayList(materialized));
        pipeline.done();
    }

    /**
     * 测试从SequenceFile读取值
     *
     * @ignore 因为Crunch错误地假设key是NullWritable
     */
    @Test
    @Disabled("fails since Crunch (incorrectly) assumes key is NullWritable")
    public void testReadValuesFromSequenceFile() throws IOException {
        String inputPath = tmpDir.copyResourceFileName("numbers.seq");
        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> lines = pipeline.read(From.sequenceFile(inputPath,
            Writables.strings()));
        Iterable<String> materialized = lines.materialize();
        assertTrue(Lists.newArrayList(materialized).contains("One, two, buckle my shoe"));
        pipeline.done();
    }

    /**
     * 测试读取PTable from SequenceFile
     */
    @Test
    public void testReadPTableFromSequenceFile() throws IOException {
        String inputPath = tmpDir.copyResourceFileName("numbers.seq");
        Pipeline pipeline = new MRPipeline(getClass());
        TableSource<Integer, String> source =
            From.sequenceFile(inputPath, Writables.ints(), Writables.strings());
        PTable<Integer, String> table = pipeline.read(source);
        Map<Integer, String> map = table.materializeToMap();
        assertEquals("Nine, ten, a big fat hen", map.get(1));
        pipeline.done();
    }

    /**
     * 测试使用Writable类型读取SequenceFile
     */
    @Test
    public void testReadPTableFromSequenceFileAsWritables() throws IOException {
        String inputPath = tmpDir.copyResourceFileName("numbers.seq");
        Pipeline pipeline = new MRPipeline(getClass());
        TableSource<IntWritable, Text> source =
            From.sequenceFile(inputPath, IntWritable.class, Text.class);
        PTable<IntWritable, Text> table = pipeline.read(source);
        Map<IntWritable, Text> map = table.materializeToMap();
        assertEquals(new Text("Nine, ten, a big fat hen"), map.get(new IntWritable(1)));
        pipeline.done();
    }

//  @Test
//  public void testReadFromAvroFile() throws IOException {
//    String inputPath = tmpDir.copyResourceFileName("weather.avro");
//    Pipeline pipeline = new MRPipeline(getClass());
//    Source<WeatherRecord> source =
//        From.avroFile(inputPath, Avros.specifics(WeatherRecord.class));
//    PCollection<WeatherRecord> records = pipeline.read(source);
//    assertEquals((Long) 10L, records.length().getValue());
//    pipeline.done();
//  }

    /**
     * 测试Writable类型写入Avro文件会失败
     */
    @Test
    public void testWriteWritablesToAvroFileFails() throws IOException {
        String inputPath = tmpDir.copyResourceFileName("ints.txt");
        String outputPath = tmpDir.getFileName("out");
        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> lines = pipeline.read(From.textFile(inputPath));
        lines.write(To.avroFile(outputPath));
        assertThrows(CrunchRuntimeException.class, () -> pipeline.done());
    }

    /**
     * 测试在内存管道中写入Avro文件
     */
    @Test
    public void testWritePTableToAvroFileInMem() throws IOException {
        PCollection<String> lines = MemPipeline.typedCollectionOf(Avros.strings(), "2", "3", "1", "3");
        PTable<Integer, String> table = lines.by(new MapFn<String, Integer>() {
            @Override
            public Integer map(String input) {
                return input.length();
            }
        }, Avros.ints());
        table.write(To.avroFile("/tmp/out"), Target.WriteMode.OVERWRITE);
        lines.getPipeline().done();
    }

    /**
     * 测试PTable写入Avro文件
     */
    @Test
    public void testWritePTableToAvroFile() throws IOException {
        String inputPath = tmpDir.copyResourceFileName("ints.txt");
        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> lines = pipeline.read(From.textFile(inputPath, Avros.strings()));
        PTable<Integer, String> table = lines.by(new MapFn<String, Integer>() {
            @Override
            public Integer map(String input) {
                return input.length();
            }
        }, Avros.ints());
        table.write(To.avroFile("/tmp/out2"), Target.WriteMode.OVERWRITE);
        pipeline.done();
    }

}