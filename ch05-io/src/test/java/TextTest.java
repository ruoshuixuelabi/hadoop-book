// == TextTest
// == TextTest-Find
// == TextTest-Mutability
// == TextTest-ByteArrayNotShortened
// == TextTest-ToString
// == TextTest-Comparison
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

/**
 * 验证 {@link org.apache.hadoop.io.Text} 的各种行为
 *
 * <p>{@code Text} 是 Hadoop 里最常用的字符串类型，
 * 它按 UTF-8 存储，因此可以比较、可以排序、
 * 可以作为 MapReduce 的键
 * <p>本测试覆盖长度、查找、可变性、缓冲区行为、
 * 字符串化、比较规则以及增补平面字符的处理
 */
public class TextTest extends WritableTestBase {
  
  /**
   * 验证长度与字符访问
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void test() throws IOException {
    // vv TextTest
    Text t = new Text("hadoop");
    assertThat(t.getLength(), is(6));
    // 纯 ASCII 字符串，字节数与字符数相同
    assertThat(t.getBytes().length, is(6));
    
    assertThat(t.charAt(2), is((int) 'd'));
    // 越界访问返回 -1，而不是抛异常
    assertThat("Out of bounds", t.charAt(100), is(-1));
    // ^^ TextTest
  }    

  /**
   * 验证子串查找
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void find() throws IOException {
    // vv TextTest-Find
    Text t = new Text("hadoop");
    assertThat("Find a substring", t.find("do"), is(2));
    assertThat("Finds first 'o'", t.find("o"), is(3));
    assertThat("Finds 'o' from position 4 or later", t.find("o", 4), is(4));
    // 找不到时返回 -1
    assertThat("No match", t.find("pig"), is(-1));
    // ^^ TextTest-Find
  }
  
  /**
   * 验证 {@code Text} 是可变的
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void mutability() throws IOException {
    // vv TextTest-Mutability
    Text t = new Text("hadoop");
    t.set("pig");
    assertThat(t.getLength(), is(3));
    // set 会把内容整体替换掉，长度随之改变
    assertThat(t.getBytes().length, is(3));
    // ^^ TextTest-Mutability
  }
  
  /**
   * 验证内容变短时底层缓冲区不会被缩回
   *
   * <p>{@code Text} 只维护「有效长度」，
   * 底层 {@code byte[]} 数组只增不减——这是 Hadoop 序列化里
   * 常见的做法：避免频繁分配内存
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void byteArrayNotShortened() throws IOException {
    // vv TextTest-ByteArrayNotShortened
    Text t = new Text("hadoop");
    t.set(/*[*/new Text("pig")/*]*/);
    assertThat(t.getLength(), is(3));
    // 有效长度降到 3，但底层数组仍是当初 6 字节的那个
    assertThat("Byte length not shortened", t.getBytes().length,
      /*[*/is(6)/*]*/);
    // ^^ TextTest-ByteArrayNotShortened
  }

  /**
   * 验证 {@code toString()} 返回的是原始字符串
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void toStringMethod() throws IOException {
    // vv TextTest-ToString
    assertThat(new Text("hadoop").toString(), is("hadoop"));
    // ^^ TextTest-ToString
  }
  
  /**
   * 验证 {@code Text} 与 {@code String} 的比较结果并不一致
   *
   * <p>增补平面字符在 UTF-16 里用代理对表示，
   * 按码元比时 {@code \uD800} 小于 {@code \ue000}；
   * 而按码点比时 U+10000 远大于 U+E000
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void comparison() throws IOException {
    assertThat("\ud800\udc00".compareTo("\ue000"), lessThan(0));
    // Text 的比较结果与 String 相反
    assertThat(new Text("\ud800\udc00").compareTo(new Text("\ue000")), greaterThan(0));
  }
  
  /**
   * 验证增补平面字符在 {@code Text} 中的处理
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void withSupplementaryCharacters() throws IOException {
    
    String s = "\u0041\u00DF\u6771\uD801\uDC00";
    assertThat(s.length(), is(5));
    assertThat(s.getBytes("UTF-8").length, is(10));
    
    assertThat(s.indexOf('\u0041'), is(0));
    assertThat(s.indexOf('\u00DF'), is(1));
    assertThat(s.indexOf('\u6771'), is(2));
    assertThat(s.indexOf('\uD801'), is(3));
    // String 里两个代理码元各占一个位置
    assertThat(s.indexOf('\uDC00'), is(4));
    
    assertThat(s.charAt(0), is('\u0041'));
    assertThat(s.charAt(1), is('\u00DF'));
    assertThat(s.charAt(2), is('\u6771'));
    assertThat(s.charAt(3), is('\uD801'));
    assertThat(s.charAt(4), is('\uDC00'));
    
    Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
    
    // 序列化后：0a 是长度 10，41 是 A，c39f 是 ü，e69db1 是汉字，
    // f0909080 是增补字符的 UTF-8 编码（4 字节）
    assertThat(serializeToString(t), is("0a41c39fe69db1f0909080"));
    
    assertThat(t.charAt(t.find("\u0041")), is(0x0041));
    assertThat(t.charAt(t.find("\u00DF")), is(0x00DF));
    assertThat(t.charAt(t.find("\u6771")), is(0x6771));
    // Text 把代理对合并成单个码点 0x10400
    assertThat(t.charAt(t.find("\uD801\uDC00")), is(0x10400));
    
  }    

}
