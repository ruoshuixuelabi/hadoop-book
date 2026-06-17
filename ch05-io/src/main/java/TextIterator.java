// cc TextIterator Iterating over the characters in a Text object
// cc TextIterator 演示 Text 对象中的字符迭代

import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;

// vv TextIterator
/**
 * TextIterator - 演示 Text 对象的字符迭代
 *
 * <p>此程序演示如何正确遍历 Hadoop Text 对象中的 Unicode 字符。
 * Text 使用 UTF-8 编码存储字符串,与 Java 的 String 有很大不同。</p>
 *
 * <p><b>Text vs String 字符表示:</b></p>
 * <ul>
 *   <li><b>String</b>: 使用 UTF-16 编码,charAt() 返回 char (16-bit)</li>
 *   <li><b>Text</b>: 使用 UTF-8 编码,charAt() 返回 int (Unicode code point)</li>
 *   <li>对于 BMP 字符(大多数常用字),两者表示相同</li>
 *   <li>对于辅助字符(Unicode 5.0+),Text 更节省空间</li>
 * </ul>
 *
 * <p><b>测试数据说明:</b></p>
 * <ul>
 *   <li>A: 拉丁字母 A (1 个字节 UTF-8)</li>
 *   <li>ß: 拉丁字母 sharp S (2 个字节 UTF-8)</li>
 *   <li>東: 汉字"东" (3 个字节 UTF-8)</li>
 *   <li>𐐀: 斯拉夫字母 (4 个字节 UTF-8,代理对)</li>
 * </ul>
 *
 * <p><b>输出说明:</b></p>
 * <p>程序会打印每个字符的 Unicode 码点(十六进制格式):</p>
 * <ul>
 *   <li>41 - 拉丁字母 A</li>
 *   <li>df - 拉丁 sharp S</li>
 *   <li>6771 - 汉字"东"</li>
 *   <li>10400 - 斯拉夫字母(来自代理对)</li>
 * </ul>
 *
 * @see Text
 * @see StringTextComparisonTest
 */
public class TextIterator {

    /**
     * 主方法 - 演示 Text 字符迭代
     *
     * <p><b>算法步骤:</b></p>
     * <ol>
     *   <li>创建一个包含多种 Unicode 字符的 Text 对象</li>
     *   <li>获取 Text 的字节缓冲区和长度</li>
     *   <li>循环调用 Text.bytesToCodePoint() 获取每个字符</li>
 *       <li>打印每个字符的十六进制 Unicode 码点</li>
     * </ol>
     *
     * <p><b>bytesToCodePoint 行为:</b></p>
     * <ul>
     *   <li>自动识别 UTF-8 编码的字符边界</li>
     *   <li>处理单字节、多字节和代理对字符</li>
     *   <li>返回 -1 表示已到达缓冲区末尾</li>
     * </ul>
     *
     * @param args 命令行参数 (未使用)
     */
    public static void main(String[] args) {
        // 创建包含多种 Unicode 字符的 Text 对象
        // A = 'A' (1 byte UTF-8: 0x41)
        // ß = 'ß' (2 byte UTF-8: 0xC3 0x9F)
        // 東 = '东' (3 byte UTF-8: 0xE5 0x85 0xB1)
        // 𐐀 = Linear B Syllable B008 A (4 byte UTF-8: 0xF0 0x90 0x90 0x80)
        Text t = new Text("Aß東𐐀");

        // 创建字节缓冲区,包含 Text 的实际字节内容
        // t.getBytes() 返回内部字节数组
        // t.getLength() 返回实际使用的字节数
        ByteBuffer buf = ByteBuffer.wrap(t.getBytes(), 0, t.getLength());

        int cp;  // Unicode code point (码点)
        // 循环遍历每个字符
        // bytesToCodePoint 会自动识别 UTF-8 边界
        while (buf.hasRemaining() && (cp = Text.bytesToCodePoint(buf)) != -1) {
            // 以十六进制格式打印 Unicode 码点
            System.out.println(Integer.toHexString(cp));
        }
    }
}
// ^^ TextIterator
