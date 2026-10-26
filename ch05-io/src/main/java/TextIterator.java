// cc TextIterator Iterating over the characters in a Text object
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;

// vv TextIterator
/**
 * 演示如何遍历 {@link Text} 里的 Unicode 码点
 *
 * <p>{@link Text} 内部是 UTF-8 字节数组，一个字符可能占 1~4 个字节，
 * 所以不能用「一个字节一个字符」的方式遍历。
 * 正确做法是先用 {@link java.nio.ByteBuffer} 包住字节，
 * 再用 {@link Text#bytesToCodePoint(ByteBuffer)} 逐个解出码点
 * <p>本例特意用了 ASCII、德语 ß、日语東、以及一个增补平面字符，
 * 覆盖 UTF-8 的 1/2/3/4 字节四种情形
 */
public class TextIterator {
  
  /**
   * 遍历并打印每个 Unicode 码点的十六进制形式
   *
   * @param args 未使用
   */
  public static void main(String[] args) {    
    Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
    
    // 只包装前 getLength() 个字节，避免把缓冲区末尾的空闲字节也算进去
    ByteBuffer buf = ByteBuffer.wrap(t.getBytes(), 0, t.getLength());
    // hasRemaining 判断是否还有字节；bytesToCodePoint 返回 -1 表示解不出码点
    int cp;
    // 每次循环解出一个码点并打印
    while (buf.hasRemaining() && (cp = Text.bytesToCodePoint(buf)) != -1) {
      System.out.println(Integer.toHexString(cp));
    }
  }  
}
// ^^ TextIterator
