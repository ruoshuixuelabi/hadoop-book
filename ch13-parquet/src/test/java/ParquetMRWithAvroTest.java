import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import parquet.example.data.Group;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThat;

/**
 * Parquet MapReduce 作业测试类 (使用 Avro API)。
 *
 * <p>测试完整的 MapReduce 流程：
 * <ul>
 *   <li>TextToParquetWithAvro: 文本 -> Parquet (使用 Avro 输出格式)</li>
 *   <li>ParquetToTextWithAvro: Parquet -> 文本 (使用 Avro 输入格式)</li>
 * </ul>
 *
 * <p>与 {@link ParquetMRWithExampleTest} 的对比：
 * <ul>
 *   <li>使用 AvroParquetOutputFormat/AvroParquetInputFormat</li>
 *   <li>输出为 JSON 格式的 Avro 记录</li>
 *   <li>可以与 Avro Schema 集成</li>
 * </ul>
 */
public class ParquetMRWithAvroTest {
    @TempDir
    public transient Path tmpDir;

    /**
     * 测试文本到 Parquet (Avro) 再到文本的完整转换流程。
     *
     * <p>流程：
     * <ol>
     *   <li>创建输入文本文件 (fruit.txt)</li>
     *   <li>运行 TextToParquetWithAvro 转换为 Parquet</li>
     *   <li>直接读取 Parquet 文件验证内容</li>
     *   <li>运行 ParquetToTextWithAvro 转换回文本</li>
     *   <li>验证最终文本输出 (JSON 格式)</li>
     * </ol>
     *
     * <p>输入文件内容: "cherry", "apple", "banana"
     * <p>输出格式: {"offset": 0, "line": "cherry"} 等 JSON 格式
     */
    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();

        // 创建输入文件
        Path inputFile = tmpDir.resolve("input").toFile().toPath();
        Files.copy(Resources.newInputStreamSupplier(Resources.getResource("fruit.txt")),
            inputFile.toFile());
        Path parquetOutputFolder = tmpDir.resolve("output-parquet").toFile().toPath();
        parquetOutputFolder.toFile().delete();
        Path textOutputFolder = tmpDir.resolve("output-text").toFile().toPath();
        textOutputFolder.toFile().delete();

        Path input = inputFile;
        Path parquetOutput = parquetOutputFolder;
        Path textOutput = textOutputFolder;

        // 步骤 1: 文本 -> Parquet (使用 Avro)
        int rc = runTool(new TextToParquetWithAvro(), conf, input.toString(), parquetOutput.toString());
        assertEquals(0, rc);

        // 步骤 2: 读取 Parquet 文件验证
        Path parquetFile = new Path(parquetOutput, "part-m-00000.parquet");
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = new ParquetReader<Group>(parquetFile, readSupport);

        // 验证 Parquet 中的数据
        checkNextGroup(reader, 0, "cherry");
        checkNextGroup(reader, 7, "apple");
        checkNextGroup(reader, 13, "banana");
        assertNull(reader.read());

        // 步骤 3: Parquet -> 文本 (使用 Avro，输出 JSON)
        rc = runTool(new ParquetToTextWithAvro(), conf, parquetOutput.toString(), textOutput.toString());
        assertEquals(0, rc);

        // 步骤 4: 验证最终文本输出 (JSON 格式)
        assertEquals(
            "{\"offset\": 0, \"line\": \"cherry\"}\n" +
                "{\"offset\": 7, \"line\": \"apple\"}\n" +
                "{\"offset\": 13, \"line\": \"banana\"}\n",
            Files.toString(new File(new Path(textOutput, "part-m-00000").toUri()),
                Charsets.UTF_8));
    }

    /**
     * 运行 Tool 接口的工具类。
     */
    private int runTool(Tool tool, Configuration conf, String... args) throws Exception {
        tool.setConf(conf);
        return tool.run(args);
    }

    /**
     * 验证 Parquet 文件中下一条记录的内容。
     */
    private void checkNextGroup(ParquetReader<Group> reader, int offset, String line) throws IOException {
        Group result = reader.read();
        assertNotNull(result);
        assertThat(result.getValueToString(0, 0), is(offset + ""));
        assertThat(result.getString("line", 0), is(line));
    }
}