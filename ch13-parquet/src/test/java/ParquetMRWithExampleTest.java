import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * 「文本 → Parquet → 文本」的往返测试
 *
 * <p>先用 Parquet 自己的输出格式把文本转成 Parquet，
 * 再用它自己的输入格式转回文本，
 * 最后比较转换前后的内容是否一致
 * <p>走的是 Parquet 示例接口，
 * 所以读回来的记录是「字段名: 值」的形式
 */
public class ParquetMRWithExampleTest {
  /**
   * 临时目录：每个用例都用它来存放输入与输出
   *
   * <p>由 JUnit 在每个用例前后创建与删除，用例之间互不干扰
   */
  @Rule
  public transient TemporaryFolder tmpDir = new TemporaryFolder();

  /**
   * 跑一遍完整的往返转换
   *
   * @throws Exception 作业执行失败
   */
  @Test
  public void test() throws Exception {
    // 作业配置：用默认的即可
    Configuration conf = new Configuration();

    // 把测试数据复制到临时目录，作业需要的是一个真实的文件
    File inputFile = tmpDir.newFile("input");
    // fruit.txt 是测试资源，每行一个水果名
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("fruit.txt")),
        inputFile);
    // 输出目录必须事先不存在，作业才会创建它
    File parquetOutputFolder = tmpDir.newFolder("output-parquet");
    parquetOutputFolder.delete();
    // 同理，文本输出目录也要先删掉
    File textOutputFolder = tmpDir.newFolder("output-text");
    textOutputFolder.delete();

    // 转换成 Hadoop 的路径对象
    Path input = new Path(inputFile.toURI());
    // Parquet 输出的路径
    Path parquetOutput = new Path(parquetOutputFolder.toURI());
    // 文本输出的路径
    Path textOutput = new Path(textOutputFolder.toURI());

    // 第一步：文本转 Parquet
    int rc = runTool(new TextToParquetWithExample(), conf, input.toString(), parquetOutput.toString());
    // 0 表示作业成功
    assertEquals(0, rc);

    // Map 任务的输出文件名是固定的，可以直接拼出来
    Path parquetFile = new Path(parquetOutput, "part-m-00000.parquet");
    // 读取用的支持类：告诉 Parquet 按哪种方式反序列化
    GroupReadSupport readSupport = new GroupReadSupport();
    // 打开 Parquet 文件的读取器
    ParquetReader<Group> reader = new ParquetReader<Group>(parquetFile, readSupport);

    // 逐条核对写进去的内容
    checkNextGroup(reader, 0, "cherry");
    checkNextGroup(reader, 7, "apple");
    checkNextGroup(reader, 13, "banana");
    // 读完了，返回 null
    assertNull(reader.read());

    // 第二步：Parquet 转回文本
    rc = runTool(new ParquetToTextWithExample(), conf, parquetOutput.toString(), textOutput.toString());
    // 0 表示作业成功
    assertEquals(0, rc);

    // 比较转换前后的文本是否完全一致
    // Parquet 示例接口读出来的是「字段名: 值」的形式，
    // 输出文件名是 part-m-00000（没有扩展名）
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
   * 运行一个作业
   *
   * <p>{@link Tool} 是所有作业类的共同父类，
   * 这里统一用它来调用，避免每个用例各写一遍
   *
   * @param tool 要运行的作业
   * @param conf 作业配置
   * @param args 作业参数
   * @return 作业的退出码
   * @throws Exception 作业执行失败
   */
  private int runTool(Tool tool, Configuration conf, String... args) throws Exception {
    // 工具类需要先拿到配置
    tool.setConf(conf);
    // 再真正执行
    return tool.run(args);
  }

  /**
   * 读一条记录并核对它的内容
   *
   * @param reader Parquet 读取器
   * @param offset 期望的行号
   * @param line 期望的文本
   * @throws IOException 读取失败
   */
  private void checkNextGroup(ParquetReader<Group> reader, int offset, String line) throws IOException {
    // 读出下一条记录
    Group result = reader.read();
    // 必须读到了记录
    assertNotNull(result);
    // 第 0 个字段的第 0 个值：行号
    assertThat(result.getValueToString(0, 0), is(offset + ""));
    // 名为 line 的字段：整行文本
    assertThat(result.getString("line", 0), is(line));
  }
}
