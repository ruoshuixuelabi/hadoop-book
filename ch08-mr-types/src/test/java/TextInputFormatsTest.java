/**
 * TextInputFormat 各种变体的测试类。
 *
 * <p>本测试类验证三种不同的文本输入格式：
 * <ul>
 *   <li>{@link org.apache.hadoop.mapreduce.lib.input.TextInputFormat} - 默认的按行输入格式</li>
 *   <li>{@link org.apache.hadoop.mapred.lib.KeyValueTextInputFormat} - 键值对输入格式</li>
 *   <li>{@link org.apache.hadoop.mapred.lib.NLineInputFormat} - 按固定行数分割的输入格式</li>
 * </ul>
 *
 * <p>测试覆盖的场景：
 * <ul>
 *   <li>TextInputFormat：验证行偏移量和行内容</li>
 *   <li>KeyValueTextInputFormat：验证自定义键值分隔符的解析</li>
 *   <li>NLineInputFormat：验证按固定行数分割的逻辑</li>
 * </ul>
 *
 * <p>测试方法：
 * <ul>
 *   <li>text() - 测试标准的 TextInputFormat</li>
 *   <li>keyValue() - 测试 KeyValueTextInputFormat</li>
 *   <li>nLine() - 测试 NLineInputFormat</li>
 * </ul>
 */
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.junit;


/**
 * TextInputFormat 各种变体的单元测试。
 *
 * <p>使用 JUnit 4 进行测试，每个测试方法验证一种输入格式的行为。
 */
public class TextInputFormatsTest {

    // 测试用的基础路径
    private static final String BASE_PATH = "/tmp/" +
        TextInputFormatsTest.class.getSimpleName();

    // Hadoop 作业配置
    private JobConf conf;
    // HDFS 文件系统实例
    private FileSystem fs;

    /**
     * 测试前置方法，创建配置和文件系统。
     *
     * @throws Exception 如果初始化失败
     */
    @Before
    public void setUp() throws Exception {
        conf = new JobConf();
        fs = FileSystem.get(conf);
    }

    /**
     * 测试后置方法，清理测试数据。
     *
     * @throws Exception 如果清理失败
     */
    @After
    public void tearDown() throws Exception {
        // 删除测试生成的目录
        fs.delete(new Path(BASE_PATH), true);
    }

    /**
     * 测试标准的 TextInputFormat。
     *
     * <p>TextInputFormat 是 Hadoop 的默认输入格式，
     * 它按行读取文件，每一行作为一条记录。
     *
     * <p>键：行在文件中的偏移量（LongWritable）
     * <p>值：行的内容（Text）
     *
     * <p>测试数据：
     * <pre>
     * "On the top of the Crumpetty Tree\n"     -> key=0,  长度33
     * "The Quangle Wangle sat,\n"              -> key=33, 长度24
     * "But his face you could not see,\n"      -> key=57, 长度32
     * "On account of his Beaver Hat."          -> key=89, 长度32
     * </pre>
     *
     * @throws Exception 如果测试失败
     */
    @Test
    public void text() throws Exception {
        // 输入文本（注意每行的长度）
        String input =
            "On the top of the Crumpetty Tree\n" +    // 33 字符（含换行符）
            "The Quangle Wangle sat,\n" +              // 24 字符
            "But his face you could not see,\n" +     // 33 字符
            "On account of his Beaver Hat.";          // 32 字符（最后一行无换行）

        // 将输入写入 HDFS
        writeInput(input);

        // 创建 TextInputFormat 实例
        TextInputFormat format = new TextInputFormat();
        format.configure(conf);
        // 获取输入分片
        InputSplit[] splits = format.getSplits(conf, 1);
        // 获取记录读取器
        RecordReader<LongWritable, Text> recordReader =
            format.getRecordReader(splits[0], conf, Reporter.NULL);

        // 验证第一条记录：偏移量 0，内容 "On the top of the Crumpetty Tree"
        checkNextLine(recordReader, 0, "On the top of the Crumpetty Tree");
        // 验证第二条记录：偏移量 33，内容 "The Quangle Wangle sat,"
        checkNextLine(recordReader, 33, "The Quangle Wangle sat,");
        // 验证第三条记录：偏移量 57，内容 "But his face you could not see,"
        checkNextLine(recordReader, 57, "But his face you could not see,");
        // 验证第四条记录：偏移量 89，内容 "On account of his Beaver Hat."
        checkNextLine(recordReader, 89, "On account of his Beaver Hat.");
    }

    /**
     * 测试 KeyValueTextInputFormat。
     *
     * <p>KeyValueTextInputFormat 允许自定义键值分隔符（默认是 Tab）。
     * 每行按分隔符分割，前半部分为键，后半部分为值。
     *
     * <p>测试数据（使用 Tab 作为键值分隔符）：
     * <pre>
     * "line1\tOn the top of the Crumpetty Tree"
     * "line2\tThe Quangle Wangle sat,"
     * "line3\tBut his face you could not see,"
     * "line4\tOn account of his Beaver Hat."
     * </pre>
     *
     * @throws Exception 如果测试失败
     */
    @Test
    public void keyValue() throws Exception {
        // 输入文本（键值对用 Tab 分隔）
        String input =
            "line1\tOn the top of the Crumpetty Tree\n" +
            "line2\tThe Quangle Wangle sat,\n" +
            "line3\tBut his face you could not see,\n" +
            "line4\tOn account of his Beaver Hat.";

        // 将输入写入 HDFS
        writeInput(input);

        // 创建 KeyValueTextInputFormat 实例
        KeyValueTextInputFormat format = new KeyValueTextInputFormat();
        format.configure(conf);
        // 获取输入分片
        InputSplit[] splits = format.getSplits(conf, 1);
        // 获取记录读取器
        RecordReader<Text, Text> recordReader =
            format.getRecordReader(splits[0], conf, Reporter.NULL);

        // 验证每条记录的键和值
        checkNextLine(recordReader, "line1", "On the top of the Crumpetty Tree");
        checkNextLine(recordReader, "line2", "The Quangle Wangle sat,");
        checkNextLine(recordReader, "line3", "But his face you could not see,");
        checkNextLine(recordReader, "line4", "On account of his Beaver Hat.");
    }

    /**
     * 测试 NLineInputFormat。
     *
     * <p>NLineInputFormat 将文件按固定行数分割为多个分片。
     * 每个 Map 任务处理 N 行，而不是一个完整的文件。
     *
     * <p>本测试设置每 2 行一个分片：
     * <pre>
     * 分片 0：第 1-2 行（"On the top..." 和 "The Quangle..."）
     * 分片 1：第 3-4 行（"But his face..." 和 "On account..."）
     * </pre>
     *
     * @throws Exception 如果测试失败
     */
    @Test
    public void nLine() throws Exception {
        String input =
            "On the top of the Crumpetty Tree\n" +
            "The Quangle Wangle sat,\n" +
            "But his face you could not see,\n" +
            "On account of his Beaver Hat.";

        writeInput(input);

        // 设置每 2 行作为一个分片
        conf.setInt("mapred.line.input.format.linespermap", 2);
        NLineInputFormat format = new NLineInputFormat();
        format.configure(conf);
        // 获取分片（应该有 2 个分片，每个包含 2 行）
        InputSplit[] splits = format.getSplits(conf, 2);
        // 获取第一个分片的记录读取器
        RecordReader<LongWritable, Text> recordReader =
            format.getRecordReader(splits[0], conf, Reporter.NULL);

        // 验证第一个分片的两条记录
        checkNextLine(recordReader, 0, "On the top of the Crumpetty Tree");
        checkNextLine(recordReader, 33, "The Quangle Wangle sat,");

        // 获取第二个分片的记录读取器
        recordReader = format.getRecordReader(splits[1], conf, Reporter.NULL);
        // 验证第二个分片的两条记录
        checkNextLine(recordReader, 57, "But his face you could not see,");
        checkNextLine(recordReader, 89, "On account of his Beaver Hat.");
    }

    /**
     * 将测试数据写入 HDFS。
     *
     * @param input 要写入的文本内容
     * @throws IOException 如果写入失败
     */
    private void writeInput(String input) throws IOException {
        // 创建输出流写入测试文件
        OutputStream out = fs.create(new Path(BASE_PATH, "input"));
        out.write(input.getBytes());
        out.close();

        // 设置输入路径
        FileInputFormat.setInputPaths(conf, BASE_PATH);
    }

    /**
     * 验证 TextInputFormat 的下一条记录（键为 LongWritable）。
     *
     * @param recordReader   记录读取器
     * @param expectedKey    期望的键（行偏移量）
     * @param expectedValue  期望的值（行内容）
     * @throws IOException 如果读取失败
     */
    private void checkNextLine(RecordReader<LongWritable, Text> recordReader,
        long expectedKey, String expectedValue) throws IOException {
        LongWritable key = new LongWritable();
        Text value = new Text();
        // 读取下一条记录
        assertThat(expectedValue, recordReader.next(key, value), is(true));
        // 验证键（偏移量）
        assertThat(key.get(), is(expectedKey));
        // 验证值（内容）
        assertThat(value.toString(), is(expectedValue));
    }

    /**
     * 验证 KeyValueTextInputFormat 的下一条记录（键为 Text）。
     *
     * @param recordReader   记录读取器
     * @param expectedKey    期望的键
     * @param expectedValue  期望的值
     * @throws IOException 如果读取失败
     */
    private void checkNextLine(RecordReader<Text, Text> recordReader,
        String expectedKey, String expectedValue) throws IOException {
        Text key = new Text();
        Text value = new Text();
        // 读取下一条记录
        assertThat(expectedValue, recordReader.next(key, value), is(true));
        // 验证键
        assertThat(key.toString(), is(expectedKey));
        // 验证值
        assertThat(value.toString(), is(expectedValue));
    }
}
