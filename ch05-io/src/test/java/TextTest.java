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
 * TextTest - Text 的单元测试
 *
 * <p>测试 Text 的各种功能,包括:</p>
 * <ul>
 *   <li>基本操作 (长度、字节数组)</li>
 *   <li>charAt() 方法</li>
 *   <li>find() 子串查找</li>
 *   <li>可变性和容量</li>
 *   <li>与 String 的比较</li>
 *   <li>辅助字符(代理对)处理</li>
 * </ul>
 *
 * @see Text
 * @see StringTextComparisonTest
 */
public class TextTest extends WritableTestBase {

    /**
     * test - 测试 Text 基本操作
     */
    @Test
    public void test() throws IOException {
        // vv TextTest
        // 创建 Text 对象
        Text t = new Text("hadoop");

        // 验证长度和字节数组长度
        assertThat(t.getLength(), is(6));
        assertThat(t.getBytes().length, is(6));

        // charAt 返回 Unicode 码点
        assertThat(t.charAt(2), is((int) 'd'));

        // 越界访问返回 -1
        assertThat("Out of bounds", t.charAt(100), is(-1));
        // ^^ TextTest
    }

    /**
     * find - 测试子串查找
     */
    @Test
    public void find() throws IOException {
        // vv TextTest-Find
        Text t = new Text("hadoop");

        // 查找子串位置
        assertThat("Find a substring", t.find("do"), is(2));
        assertThat("Finds first 'o'", t.find("o"), is(3));
        assertThat("Finds 'o' from position 4 or later", t.find("o", 4), is(4));
        assertThat("No match", t.find("pig"), is(-1));
        // ^^ TextTest-Find
    }

    /**
     * mutability - 测试 Text 的可变性
     */
    @Test
    public void mutability() throws IOException {
        // vv TextTest-Mutability
        Text t = new Text("hadoop");
        // 使用 set() 方法修改内容
        t.set("pig");
        // 长度更新
        assertThat(t.getLength(), is(3));
        assertThat(t.getBytes().length, is(3));
        // ^^ TextTest-Mutability
    }

    /**
     * byteArrayNotShortened - 测试容量不会自动缩短
     */
    @Test
    public void byteArrayNotShortened() throws IOException {
        // vv TextTest-ByteArrayNotShortened
        Text t = new Text("hadoop");

        // 使用新的 Text 对象设置内容
        // 注意: 传入的是新的 Text 对象,不是 String
        t.set(/*[*/new Text("pig")/*]*/);

        // 实际长度更新
        assertThat(t.getLength(), is(3));

        // 但内部字节数组长度不变 (容量管理优化)
        assertThat("Byte length not shortened", t.getBytes().length,
          /*[*/is(6)/*]*/);
        // ^^ TextTest-ByteArrayNotShortened
    }

    /**
     * toStringMethod - 测试 toString 方法
     */
    @Test
    public void toStringMethod() throws IOException {
        // vv TextTest-ToString
        assertThat(new Text("hadoop").toString(), is("hadoop"));
        // ^^ TextTest-ToString
    }

    /**
     * comparison - 测试 Text 与 String 的排序比较差异
     */
    @Test
    public void comparison() throws IOException {
        // String 使用 UTF-16,Text 使用 UTF-8
        // 代理对字符的排序顺序不同
        assertThat("𐀀".compareTo(""), lessThan(0));
        assertThat(new Text("𐀀").compareTo(new Text("")), greaterThan(0));
    }

    /**
     * withSupplementaryCharacters - 测试辅助字符(4字节 Unicode)
     *
     * <p>Text 正确处理 4 字节的 Unicode 字符 (如古代文字)。</p>
     */
    @Test
    public void withSupplementaryCharacters() throws IOException {

        // 测试字符串
        String s = "Aß東𐐀";
        assertThat(s.length(), is(5));  // String length: 5 chars
        assertThat(s.getBytes("UTF-8").length, is(10));  // UTF-8 bytes: 10

        // String.charAt() 返回 char (16-bit)
        assertThat(s.indexOf('A'), is(0));
        assertThat(s.indexOf('ß'), is(1));
        assertThat(s.indexOf('東'), is(2));
        assertThat(s.indexOf('\uD801'), is(3));
        assertThat(s.indexOf('\uDC00'), is(4));

        // String.charAt() 返回 char
        assertThat(s.charAt(0), is('A'));
        assertThat(s.charAt(1), is('ß'));
        assertThat(s.charAt(2), is('東'));
        assertThat(s.charAt(3), is('\uD801'));
        assertThat(s.charAt(4), is('\uDC00'));

        // 创建 Text
        Text t = new Text("Aß東𐐀");

        // Text.getLength() 返回字节数
        assertThat(serializeToString(t), is("0a41c39fe69db1f0909080"));

        // Text.charAt() 返回 Unicode 码点 (int)
        assertThat(t.charAt(t.find("A")), is(0x0041));
        assertThat(t.charAt(t.find("ß")), is(0x00DF));
        assertThat(t.charAt(t.find("東")), is(0x6771));
        assertThat(t.charAt(t.find("𐐀")), is(0x10400));

    }

}
