import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.junit.*;


/**
 * 文本输入格式的单元测试
 *
 * <p>本测试覆盖 v1 API 下的三种文本输入格式：
 * {@code TextInputFormat}（按行切分，键是字节偏移）、
 * {@code KeyValueTextInputFormat}（按制表符切出键值）
 * 与 {@code NLineInputFormat}（每 N 行合成一条记录）
 * <p>用 JUnit 的 {@code @Before} / {@code @After} 在每个用例前后
 * 建好并清理临时目录
 */
public class TextInputFormatsTest {
  
  // 测试用的临时目录，每个用例前后都会重建
  private static final String BASE_PATH = "/tmp/" +
    TextInputFormatsTest.class.getSimpleName();
  
  // v1 的作业配置
  private JobConf conf;
  // 作业所对应的文件系统
  private FileSystem fs;
  
  /**
   * 每个用例开始前的准备动作
   *
   * <p>建立一份 {@link JobConf}，再取得它对应的文件系统
   *
   * @throws Exception 初始化失败
   */
  @Before
  public void setUp() throws Exception {
    // 建立一份作业配置
    conf = new JobConf();
    // 取得配置对应的文件系统；单元测试通常跑在本地文件系统上
    fs = FileSystem.get(conf);
  }
  
  /**
   * 每个用例结束后的清理动作
   *
   * @throws Exception 删除临时目录失败
   */
  @After
  public void tearDown() throws Exception {
    // 把临时目录整个删掉，第二个参数表示递归删除
    fs.delete(new Path(BASE_PATH), true);
  }

  /**
   * 验证 {@code TextInputFormat} 按行切分
   *
   * <p>键是每一行在文件中的字节偏移，值是这一行的文本
   *
   * @throws Exception 读写失败
   */
  @Test
  public void text() throws Exception {
    // 测试用的四行文本
    String input =
      "On the top of the Crumpetty Tree\n" +
      "The Quangle Wangle sat,\n" +
      "But his face you could not see,\n" +
      "On account of his Beaver Hat.";
    
    // 把输入写进临时目录
    writeInput(input);
    
    // 逐行读取的输入格式
    TextInputFormat format = new TextInputFormat();
    // 把配置传给输入格式
    format.configure(conf);
    // 以 1 字节为目标切分，实际上整个文件就是一片
    InputSplit[] splits = format.getSplits(conf, 1);
    // 取出第一片的读取器；v1 里读取器要连同配置一起创建
    RecordReader<LongWritable, Text> recordReader =
      format.getRecordReader(splits[0], conf, Reporter.NULL);
    // 逐行核对：键是字节偏移，值是这一行的文本
    checkNextLine(recordReader, 0, "On the top of the Crumpetty Tree");
    checkNextLine(recordReader, 33, "The Quangle Wangle sat,");
    checkNextLine(recordReader, 57, "But his face you could not see,");
    checkNextLine(recordReader, 89, "On account of his Beaver Hat.");
  }

  /**
   * 验证 {@code KeyValueTextInputFormat} 按制表符切出键值
   *
   * <p>输入的每一行都是「键\t值」，读出来键和值分别是两半
   *
   * @throws Exception 读写失败
   */
  @Test
  public void keyValue() throws Exception {
    // 每行都是「键\t值」的形式
    String input =
      "line1\tOn the top of the Crumpetty Tree\n" +
      "line2\tThe Quangle Wangle sat,\n" +
      "line3\tBut his face you could not see,\n" +
      "line4\tOn account of his Beaver Hat.";
    
    // 把输入写进临时目录
    writeInput(input);
    
    // 以制表符分隔键值的输入格式
    KeyValueTextInputFormat format = new KeyValueTextInputFormat();
    // 把配置传给输入格式
    format.configure(conf);
    // 以 1 字节为目标切分，实际上整个文件就是一片
    InputSplit[] splits = format.getSplits(conf, 1);
    // 取出第一片的读取器；v1 里读取器要连同配置一起创建
    RecordReader<Text, Text> recordReader =
      format.getRecordReader(splits[0], conf, Reporter.NULL);
    // 逐行核对：键值被正确地拆开了
    checkNextLine(recordReader, "line1", "On the top of the Crumpetty Tree");
    checkNextLine(recordReader, "line2", "The Quangle Wangle sat,");
    checkNextLine(recordReader, "line3", "But his face you could not see,");
    checkNextLine(recordReader, "line4", "On account of his Beaver Hat.");
  }
  
  /**
   * 验证 {@code NLineInputFormat} 每 N 行合成一条记录
   *
   * <p>设置每片 2 行之后，四行输入会被切成两片，
   * 每片读出来是一条包含两行的记录
   *
   * @throws Exception 读写失败
   */
  @Test
  public void nLine() throws Exception {
    // 测试用的四行文本
    String input =
      "On the top of the Crumpetty Tree\n" +
      "The Quangle Wangle sat,\n" +
      "But his face you could not see,\n" +
      "On account of his Beaver Hat.";
    
    // 把输入写进临时目录
    writeInput(input);
    
    // 指定每个分片包含 2 行
    conf.setInt("mapred.line.input.format.linespermap", 2);
    // 每 N 行合成一条记录的输入格式
    NLineInputFormat format = new NLineInputFormat();
    // 把配置传给输入格式
    format.configure(conf);
    // 以 2 字节为目标切分，得到两片
    InputSplit[] splits = format.getSplits(conf, 2);
    // 取出第一片的读取器；v1 里读取器要连同配置一起创建
    RecordReader<LongWritable, Text> recordReader =
      format.getRecordReader(splits[0], conf, Reporter.NULL);
    // 第一片的第一行
    checkNextLine(recordReader, 0, "On the top of the Crumpetty Tree");
    // 第一片的第二行
    checkNextLine(recordReader, 33, "The Quangle Wangle sat,");
    // 换成第二片的读取器
    recordReader = format.getRecordReader(splits[1], conf, Reporter.NULL);
    // 第二片的第一行
    checkNextLine(recordReader, 57, "But his face you could not see,");
    // 第二片的第二行
    checkNextLine(recordReader, 89, "On account of his Beaver Hat.");
  }
  
  /**
   * 把测试数据写进临时目录，并把它设为作业的输入路径
   *
   * @param input 要写入的文本内容
   * @throws IOException 写入失败
   */
  private void writeInput(String input) throws IOException {
    // 在临时目录下建一个名为 input 的文件
    OutputStream out = fs.create(new Path(BASE_PATH, "input"));
    // 写入测试数据
    out.write(input.getBytes());
    // 用完立刻关闭，释放文件句柄
    out.close();
    
    // 把临时目录设为作业的输入路径
    FileInputFormat.setInputPaths(conf, BASE_PATH);
  }
  
  /**
   * 核对读取器读到的下一条记录
   *
   * @param recordReader 读取器
   * @param expectedKey 期望的键（即字节偏移）
   * @param expectedValue 期望的值（即这一行的文本）
   * @throws IOException 读取失败
   */
  private void checkNextLine(RecordReader<LongWritable, Text> recordReader,
      long expectedKey, String expectedValue) throws IOException {
    // 准备接收键和值的容器
    LongWritable key = new LongWritable();
    // 值同样是临时容器
    Text value = new Text();
    // 先确认确实还有下一条记录
    assertThat(expectedValue, recordReader.next(key, value), is(true));
    // 再核对键与值的内容
    assertThat(key.get(), is(expectedKey));
    // 值就是这一行的文本
    assertThat(value.toString(), is(expectedValue));
  }
  
  /**
   * 核对读取器读到的下一条记录
   *
   * @param recordReader 读取器
   * @param expectedKey 期望的键（即制表符左边的那一半）
   * @param expectedValue 期望的值（即制表符右边的那一半）
   * @throws IOException 读取失败
   */
  private void checkNextLine(RecordReader<Text, Text> recordReader,
      String expectedKey, String expectedValue) throws IOException {
    // 准备接收键和值的容器
    Text key = new Text();
    // 值同样是临时容器
    Text value = new Text();
    // 先确认确实还有下一条记录
    assertThat(expectedValue, recordReader.next(key, value), is(true));
    // 再核对键与值的内容
    assertThat(key.toString(), is(expectedKey));
    // 值就是制表符右边的那一半
    assertThat(value.toString(), is(expectedValue));
  }
}
