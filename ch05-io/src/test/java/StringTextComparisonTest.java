// cc StringTextComparisonTest Tests showing the differences between the String and Text classes
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;

import org.apache.hadoop.io.Text;
import org.junit.Test;

// vv StringTextComparisonTest
/**
 * 对比 {@link String} 与 {@link org.apache.hadoop.io.Text} 在处理
 * Unicode 字符上的差异
 *
 * <p>两者的关键区别有三点：
 * <ul>
 * <li>长度语义不同：{@code String.length()} 数的是 UTF-16 码元，
 *     而 {@code Text.getLength()} 数的是 UTF-8 字节</li>
 * <li>字符语义不同：{@code String.charAt()} 取的是码元，
 *     {@code Text.charAt()} 取的是码点</li>
 * <li>比较规则不同：{@code String.compareTo} 按码元比，
 *     {@code Text.compareTo} 按码点比，
 *     所以增补平面字符的相对次序会不同</li>
 * </ul>
 */
public class StringTextComparisonTest {

  /**
   * 观察 {@code String} 的长度、字节数与码元操作
   *
   * @throws UnsupportedEncodingException 编码名不被支持
   */
  @Test
  public void string() throws UnsupportedEncodingException {
    
    // 5 个码元：两个单码元字符 + 一个 BMP 汉字 + 一对代理项（表示一个增补字符）
    String s = "\u0041\u00DF\u6771\uD801\uDC00";
    assertThat(s.length(), is(5));
    // UTF-8 字节数是 10：ASCII 1 字节、ü 2 字节、汉字 3 字节、增补字符 4 字节
    assertThat(s.getBytes("UTF-8").length, is(10));
    
    assertThat(s.indexOf("\u0041"), is(0));
    assertThat(s.indexOf("\u00DF"), is(1));
    assertThat(s.indexOf("\u6771"), is(2));
    // 代理项两个码元各占一个「字符位」，所以索引是 3 而不是 3 和 4
    assertThat(s.indexOf("\uD801\uDC00"), is(3));
    
    assertThat(s.charAt(0), is('\u0041'));
    assertThat(s.charAt(1), is('\u00DF'));
    assertThat(s.charAt(2), is('\u6771'));
    assertThat(s.charAt(3), is('\uD801'));
    // charAt 取到的是单个码元，代理项被拆成了两半
    assertThat(s.charAt(4), is('\uDC00'));
    
    assertThat(s.codePointAt(0), is(0x0041));
    assertThat(s.codePointAt(1), is(0x00DF));
    assertThat(s.codePointAt(2), is(0x6771));
    // codePointAt 才把代理项合并成一个完整的码点 0x10400
    assertThat(s.codePointAt(3), is(0x10400));
  }
  
  /**
   * 观察 {@code Text} 的长度与码点操作
   */
  @Test
  public void text() {
    
    Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
    // Text 的长度是 UTF-8 字节数（10），与 String.length() 的 5 不是一回事
    assertThat(t.getLength(), is(10));
    
    assertThat(t.find("\u0041"), is(0));
    assertThat(t.find("\u00DF"), is(1));
    // find 是在字节层面查找，汉字前面已有 1+2=3 个字节
    assertThat(t.find("\u6771"), is(3));
    // 增补字符前面已有 1+2+3=6 个字节
    assertThat(t.find("\uD801\uDC00"), is(6));

    assertThat(t.charAt(0), is(0x0041));
    assertThat(t.charAt(1), is(0x00DF));
    assertThat(t.charAt(3), is(0x6771));
    // Text.charAt 按码点返回，索引也是字节偏移，所以是 6 而不是 3
    assertThat(t.charAt(6), is(0x10400));
  }  
}
// ^^ StringTextComparisonTest
