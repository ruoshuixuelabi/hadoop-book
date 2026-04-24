// cc StringTextComparisonTest Tests showing the differences between the String and Text classes
// cc StringTextComparisonTest 演示 String 和 Text 类的区别

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;

import org.apache.hadoop.io.Text;
import org.junit.Test;

// vv StringTextComparisonTest
/**
 * StringTextComparisonTest - String 与 Text 的对比测试
 *
 * <p>展示 Java String 和 Hadoop Text 的关键区别:</p>
 *
 * <p><b>编码差异:</b></p>
 * <ul>
 *   <li>String: 使用 UTF-16 编码,每个 char 16 位</li>
 *   <li>Text: 使用 UTF-8 编码,字节数可变</li>
 * </ul>
 *
 * <p><b>辅助字符处理:</b></p>
 * <ul>
 *   <li>String: 代理对占用两个 char 位置</li>
 *   <li>Text: 代理对作为一个整体,字节数正确</li>
 * </ul>
 *
 * <p><b>索引行为:</b></p>
 * <ul>
 *   <li>String.indexOf(char): 查找 char</li>
 *   <li>Text.find(String): 查找子串,返回字节偏移</li>
 * </ul>
 *
 * @see Text
 */
public class StringTextComparisonTest {

    /**
     * string - 测试 String 的 Unicode 处理
     *
     * <p><b>测试数据:</b></p>
     * <ul>
     *   <li>"A" - 拉丁字母 A (1 char)</li>
     *   <li>"ß" - 拉丁字母 sharp S (1 char)</li>
     *   <li>"東" - 汉字"东" (1 char)</li>
     *   <li>"𐐀" - 辅助字符 (2 chars, 代理对)</li>
     * </ul>
     *
     * @throws UnsupportedEncodingException 不应发生
     */
    @Test
    public void string() throws UnsupportedEncodingException {

        // 创建包含多种 Unicode 字符的字符串
        String s = "Aß東𐐀";

        // String.length() 返回 char 数量
        assertThat(s.length(), is(5));

        // UTF-8 编码需要 10 字节
        assertThat(s.getBytes("UTF-8").length, is(10));

        // indexOf 查找字符位置
        assertThat(s.indexOf("A"), is(0));
        assertThat(s.indexOf("ß"), is(1));
        assertThat(s.indexOf("東"), is(2));
        assertThat(s.indexOf("𐐀"), is(3));

        // charAt 返回 char (可能只是代理对的一半)
        assertThat(s.charAt(0), is('A'));
        assertThat(s.charAt(1), is('ß'));
        assertThat(s.charAt(2), is('東'));
        assertThat(s.charAt(3), is('\uD801'));  // 只是代理对的前半部分
        assertThat(s.charAt(4), is('\uDC00'));  // 只是代理对的后半部分

        // codePointAt 返回完整的 Unicode 码点
        assertThat(s.codePointAt(0), is(0x0041));
        assertThat(s.codePointAt(1), is(0x00DF));
        assertThat(s.codePointAt(2), is(0x6771));
        assertThat(s.codePointAt(3), is(0x10400));  // 完整的辅助字符码点
    }

    /**
     * text - 测试 Text 的 Unicode 处理
     *
     * <p><b>关键区别:</b></p>
     * <ul>
     *   <li>getLength() 返回字节数而非字符数</li>
     *   <li>find() 返回字节偏移而非字符偏移</li>
     *   <li>charAt() 返回 Unicode 码点而非 char</li>
     * </ul>
     */
    @Test
    public void text() {

        // 创建 Text 对象
        Text t = new Text("Aß東𐐀");

        // Text.getLength() 返回字节数 (10)
        assertThat(t.getLength(), is(10));

        // Text.find() 返回字节偏移
        assertThat(t.find("A"), is(0));
        assertThat(t.find("ß"), is(1));
        assertThat(t.find("東"), is(3));  // 字节偏移,不是字符偏移
        assertThat(t.find("𐐀"), is(6));  // 4 字节字符,起始字节是 6

        // Text.charAt() 返回 Unicode 码点
        assertThat(t.charAt(0), is(0x0041));
        assertThat(t.charAt(1), is(0x00DF));
        assertThat(t.charAt(3), is(0x6771));  // 字节偏移 3 处的码点
        assertThat(t.charAt(6), is(0x10400));  // 字节偏移 6 处的码点
    }
}
// ^^ StringTextComparisonTest
