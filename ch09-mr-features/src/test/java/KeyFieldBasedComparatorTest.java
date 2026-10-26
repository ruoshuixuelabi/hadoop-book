import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.KeyFieldBasedComparator;
import org.junit.Test;


/**
 * {@link KeyFieldBasedComparator} 的单元测试
 *
 * <p>这个比较器模仿 Unix 的 {@code sort -k} 语法：
 * 用选项字符串描述「按第几个字段、怎样比较」，
 * 例如 {@code -k1} 表示只按第 1 个字段比较，
 * {@code -k1n} 表示按数值比较，
 * {@code -k1r} 表示逆序
 * <p>测试的做法是：把两行「字段1\t字段2」序列化成字节，
 * 交给比较器，断言它返回的顺序符合预期
 */
public class KeyFieldBasedComparatorTest {

  // 待比较的第一行：第 1 字段是 2，第 2 字段是 30
  Text line1 = new Text("2\t30");
  // 待比较的第二行：第 1 字段是 10，第 2 字段是 4
  Text line2 = new Text("10\t4");
  // 待比较的第三行：第 1 字段与第二行相同，第 2 字段更大
  Text line3 = new Text("10\t30");
  
  /**
   * 只用第 1 个字段比较
   *
   * <p>line1 的第 1 字段是 2，line2 的是 10，
   * 两种比较方式给出的先后顺序正好相反
   */
  @Test
  public void firstKey() throws Exception {
    // "-k1,1"：比较第 1 个字段；按字符串比 "2" > "10"，所以第一行排在后面
    check("-k1,1", line1, line2, 1);
    // "-k1"：只写起始字段，效果与 "-k1,1" 相同
    check("-k1",   line1, line2, 1);
    // "-k1.1"：另一种写法，效果与 "-k1,1" 相同，仍只比较第 1 个字段
    check("-k1.1", line1, line2, 1);
    // "-k1n"：按数值比较，2 < 10，顺序反过来了
    check("-k1n",  line1, line2, -1);
    // "-k1nr"：按数值比较再逆序，顺序又转回去
    check("-k1nr", line1, line2, 1);
  }

  /**
   * 只用第 2 个字段比较
   *
   * <p>line1 的第 2 字段是 30，line2 的是 4，
   * 同样两种比较方式给出的先后顺序正好相反
   */
  @Test
  public void secondKey() throws Exception {
    // "-k2,2"：比较第 2 个字段；按字符串比 "30" < "4"，所以第一行排在前面
    check("-k2,2", line1, line2, -1);
    // "-k2"：只写起始字段，效果与 "-k2,2" 相同
    check("-k2",   line1, line2, -1);
    // "-k2.1"：另一种写法，效果与 "-k2,2" 相同，仍只比较第 2 个字段
    check("-k2.1", line1, line2, -1);
    // "-k2n"：按数值比较，30 > 4，顺序反过来了
    check("-k2n",  line1, line2, 1);
    // "-k2nr"：按数值比较再逆序，顺序又转回去
    check("-k2nr", line1, line2, -1);
  }

  /**
   * 先按第 1 字段，再按第 2 字段比较
   *
   * <p>多个 {@code -k} 选项可以连用，
   * 前一个字段相同时才去看后一个字段
   */
  @Test
  public void firstThenSecondKey() throws Exception {
    // 第 1 字段已经分出胜负（"2" > "10"），第 2 字段不再参与
    check("-k1 -k2",   line1, line2, 1);
    // 第 1 字段相同（都是 10），于是由第 2 字段决定："4" > "30"
    check("-k1 -k2",   line2, line3, 1);
    // 这行用例被注释掉了：第 2 字段改按数值比较，4 < 30，顺序会反过来
    //check("-k1 -k2n",  line2, line3, -1);
    // 第 2 字段按数值比较再逆序，顺序又转回去
    check("-k1 -k2nr", line2, line3, 1);
  }
  
  /**
   * 断言比较器对两行数据给出的顺序
   *
   * @param options 比较器选项，即 {@code sort -k} 风格的参数字符串
   * @param l1 第一行数据
   * @param l2 第二行数据
   * @param c 期望的比较结果：小于 0、等于 0 或大于 0
   * @throws IOException 序列化失败
   */
  private void check(String options, Text l1, Text l2, int c) throws IOException {
    // 比较器的配置对象，选项就写在里面
    JobConf conf = new JobConf();
    // 把选项交给比较器
    conf.setKeyFieldComparatorOptions(options);
    // 待测的比较器
    KeyFieldBasedComparator comp = new KeyFieldBasedComparator();
    // 用配置初始化比较器
    comp.configure(conf);
    
    // 把第一行序列化成字节，Hadoop 的比较器比的是字节而不是对象
    DataOutputBuffer out1 = serialize(l1);
    // 第二行同样处理
    DataOutputBuffer out2 = serialize(l2);
    // 比较两段字节，断言结果等于期望值；options 作为断言失败时的说明
    assertThat(options, comp.compare(out1.getData(), 0, out1.getLength(), out2.getData(), 0, out2.getLength()), is(c));
  }
  
  /**
   * 把一个 {@link Writable} 序列化成字节缓冲区
   *
   * @param writable 要序列化的对象
   * @return 存放序列化结果的缓冲区
   * @throws IOException 序列化失败
   */
  public static DataOutputBuffer serialize(Writable writable) throws IOException {
    // 缓冲区，底层就是一个内存输出流
    DataOutputBuffer out = new DataOutputBuffer();
    // 在缓冲区外面包一层流式接口
    DataOutputStream dataOut = new DataOutputStream(out);
    // 把对象写进去
    writable.write(dataOut);
    // 关闭流会顺带把缓冲区里的数据刷出去
    dataOut.close();
    // 返回缓冲区供比较器读取
    return out;
  }
  
}
