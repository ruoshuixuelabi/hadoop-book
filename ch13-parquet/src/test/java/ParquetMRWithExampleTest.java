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
 * Parquet MapReduce 作业测试类 (使用 Example API)。
 *
 * <p>测试完整的 MapReduce 流程：
 * <ul>
 *   <li>TextToParquetWithExample: 文本 -> Parquet</li>
 *   <li>ParquetToTextWithExample: Parquet -> 文本</li>
 * </ul>
 *
 * <p>验证数据在转换过程中正确保持。
 */
public class ParquetMRWithExampleTest {
    @TempDir
    public transient Path tmpDir;

    /**
     * 测试文本到 Parquet 再到文本的完整转换流程。
     *
     * <p>流程：
     * <ol>
     *   <li>创建输入文本文件 (fruit.txt)</li>
     *   <li>运行 TextToParquetWithExample 转换为 Parquet</li>
     *   <li>直接读取 Parquet 文件验证内容</li>
     *   <li>运行 ParquetToTextWithExample 转换回文本</li>
     *   <li>验证最终文本输出</li>
     * </ol>
     *
     * <p>输入文件内容: "cherry", "apple", "banana" (每行一个)
     * <p>验证 Parquet 中的 offset 正确反映每行的位置
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

        // 步骤 1: 文本 -> Parquet
        int rc = runTool(new TextToParquetWithExample(), conf, input.toString(), parquetOutput.toString());
        assertEquals(0, rc);

        // 步骤 2: 读取 Parquet 文件验证
        Path parquetFile = new Path(parquetOutput, "part-m-00000.parquet");
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = new ParquetReader<Group>(parquetFile, readSupport);

        // cherry 在 offset 0, apple 在 offset 7, banana 在 offset 13
        checkNextGroup(reader, 0, "cherry");
        checkNextGroup(reader, 7, "apple");
        checkNextGroup(reader, 13, "banana");
        assertNull(reader.read());

        // 步骤 3: Parquet -> 文本
        rc = runTool(new ParquetToTextWithExample(), conf, parquetOutput.toString(), textOutput.toString());
        assertEquals(0, rc);

        // 步骤 4: 验证最终文本输出
        assertEquals(
            "offset: 0\n" +
                "line: cherry\n" +
                "\n" +
                "offset: 7\n" +
                "line: apple\n" +
                "\n" +
                "offset: 13\n" +
                "line: banana\n" +
                "\n",
            Files.toString(new File(new Path(textOutput, "part-m-00000").toUri()),
                Charsets.UTF_8));
    }

    /**
     * 运行 Tool 接口的工具类。
     *
     * @param tool Tool 实现类
     * @param conf Hadoop 配置
     * @param args 命令行参数
     * @return 工具的退出码
     */
    private int runTool(Tool tool, Configuration conf, String... args) throws Exception {
        tool.setConf(conf);
        return tool.run(args);
    }

    /**
     * 验证 Parquet 文件中下一条记录的内容。
     *
     * @param reader Parquet 阅读器
     * @param offset 期望的偏移量
     * @param line 期望的行内容
     */
    private void checkNextGroup(ParquetReader<Group> reader, int offset, String line) throws IOException {
        Group result = reader.read();
        assertNotNull(result);
        // 通过字段索引获取 offset 值
        assertThat(result.getValueToString(0, 0), is(offset + ""));
        // 通过字段名获取 line 值
        assertThat(result.getString("line", 0), is(line));
    }
}