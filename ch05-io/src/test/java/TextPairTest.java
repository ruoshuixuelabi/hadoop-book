import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.junit.Test;


/**
 * 验证 {@link org.apache.hadoop.io.TextPair} 的两个比较器
 *
 * <p>{@code TextPair} 是「两个 Text 组成的复合键」，
 * 提供了两个比较器：
 * <ul>
 * <li>{@code Comparator}：先比第一分量，再比第二分量</li>
 * <li>{@code FirstComparator}：只比第一分量</li>
 * </ul>
 * <p>特别地，{@code FirstComparator} 在两个分量的第一分量相同时返回 0，
 * 这正是「只按站号分组、组内顺序无所谓」所需要的行为
 */
public class TextPairTest extends WritableTestBase {
  
  // 样例：两个分量都较短
  private TextPair tp1 = new TextPair("a", "b");
  // 样例：两个分量互换
  private TextPair tp2 = new TextPair("b", "a");
  // 样例：第一分量与 tp1 相同，第二分量是它的前缀
  private TextPair tp3 = new TextPair("a", "ab");
  // 样例：第一分量是 tp1 第一分量的超串
  private TextPair tp4 = new TextPair("aa", "b");
  // 样例：第一分量很长，用来触发序列化时的可变长整数跨字节情形
  private TextPair tp5 = new TextPair(nTimes("a", 128), "b");
  // 样例：两个分量都很长
  private TextPair tp6 = new TextPair(nTimes("a", 128), nTimes("a", 128));
  // 样例：第一分量相同、第二分量不同，用来验证 FirstComparator 只看第一分量
  private TextPair tp7 = new TextPair(nTimes("a", 128), nTimes("b", 128));
  
  /**
   * 把字符串重复 n 次
   *
   * @param s 待重复的字符串
   * @param n 重复次数
   * @return 重复 n 次后的字符串
   */
  private static String nTimes(String s, int n) {
    StringBuilder sb = new StringBuilder(n);
    for (int i = 0; i < n; i++) {
      sb.append(s);
    }
    return sb.toString();
  }
  
  /**
   * 验证默认比较器「先比第一分量，再比第二分量」的规则
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void testComparator() throws IOException {
    // 同一个对象与自身比较，必然相等
    check(tp1, tp1, 0);
    // "a" < "b"，第一分量就分出胜负
    check(tp1, tp2, -1);
    // "a" < "aa"，第一分量分出胜负
    check(tp3, tp4, -1);
    // "b" > "aa"
    check(tp2, tp4, 1);
    // "a" < "aaa...a"（128 个 a）
    check(tp3, tp5, -1);
    // 第一分量相同（都是 128 个 a），第二分量 "b" < 128 个 a
    check(tp5, tp6, 1);
    // 第一分量相同，第二分量 "b" < 128 个 b
    check(tp5, tp7, -1);
  }

  /**
   * 验证只比较第一分量的比较器
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void testFirstComparator() throws IOException {
    // 直接 new 出一个比较器实例
    RawComparator comp = new TextPair.FirstComparator();
    check(comp, tp1, tp1, 0);
    // "a" < "b"
    check(comp, tp1, tp2, -1);
    // "a" < "aa"
    check(comp, tp3, tp4, -1);
    // "b" > "aa"
    check(comp, tp2, tp4, 1);
    // "a" < 128 个 a
    check(comp, tp3, tp5, -1);
    // 第一分量相同就返回 0，第二个分量被完全忽略
    check(comp, tp5, tp6, 0);
    // 同上：第二分量不同也不影响结果
    check(comp, tp5, tp7, 0);
  }
  
  /**
   * 用 {@code TextPair} 的默认比较器做一次比较
   *
   * @param tp1 左操作数
   * @param tp2 右操作数
   * @param c   期望的比较结果
   * @throws IOException 序列化失败
   */
  private void check(TextPair tp1, TextPair tp2, int c) throws IOException {
    // 取注册好的默认比较器
    check(WritableComparator.get(TextPair.class), tp1, tp2, c);
  }
  
  /**
   * 双向验证比较器：正序一次，逆序一次
   *
   * @param comp 比较器
   * @param tp1 左操作数
   * @param tp2 右操作数
   * @param c    期望的比较结果
   * @throws IOException 序列化失败
   */
  private void check(RawComparator comp, TextPair tp1, TextPair tp2, int c) throws IOException {
    checkOnce(comp, tp1, tp2, c);
    // 交换操作数，结果应当取反
    checkOnce(comp, tp2, tp1, -c);
  }

  /**
   * 分别用对象级比较与字节级比较验证同一个比较器
   *
   * <p>对象级比较会先把双方反序列化成对象；
   * 字节级比较则直接在序列化后的字节上比，更快，
   * 也是 Shuffle 阶段真正走的那条路
   *
   * @param comp 比较器
   * @param tp1 左操作数
   * @param tp2 右操作数
   * @param c    期望的比较结果
   * @throws IOException 序列化失败
   */
  private void checkOnce(RawComparator comp, TextPair tp1, TextPair tp2, int c) throws IOException {
    // "Object" 是断言失败时的说明文字，标明走的是对象级比较
    assertThat("Object", signum(comp.compare(tp1, tp2)), is(c));
    // 序列化两份数据，供字节级比较使用
    byte[] out1 = serialize(tp1);
    byte[] out2 = serialize(tp2);
    // "Raw" 标明走的是字节级比较；两个字节级比较必须与对象级给出相同结论
    assertThat("Raw", signum(comp.compare(out1, 0, out1.length, out2, 0, out2.length)), is(c));
  }
  
  /**
   * 把任意的比较结果归一化成 -1 / 0 / 1
   *
   * @param i 原始比较结果
   * @return -1、0 或 1
   */
  private int signum(int i) {
    // 比较器只保证符号，不保证具体数值，所以要归一化后才能断言
    return i < 0 ? -1 : (i == 0 ? 0 : 1);
  }

}
