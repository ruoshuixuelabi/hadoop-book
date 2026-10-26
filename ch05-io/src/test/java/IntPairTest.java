import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.junit.Test;

/**
 * 验证 {@link org.apache.hadoop.io.IntPair} 的比较器行为
 *
 * <p>{@code IntPair} 是「两个 int 组成的复合键」，
 * 比较时先比第一个分量，相等才比第二个分量
 * <p>本测试用一组精心挑选的样例覆盖各种分支：
 * 完全相等、只第一分量不同、只第二分量不同、
 * 以及分量取到 {@code Integer.MAX_VALUE} 的极端情况
 */
public class IntPairTest extends WritableTestBase {
  
  // 样例：两个分量都相同
  private IntPair ip1 = new IntPair(1, 2);
  // 样例：分量互换，第一分量更大
  private IntPair ip2 = new IntPair(2, 1);
  // 样例：第一分量与 ip1 相同，第二分量更大
  private IntPair ip3 = new IntPair(1, 12);
  // 样例：第一分量比 ip3 大，第二分量相同
  private IntPair ip4 = new IntPair(11, 2);
  // 样例：第一分量取到 int 上界，用来验证不会溢出
  private IntPair ip5 = new IntPair(Integer.MAX_VALUE, 2);
  // 样例：两个分量都取到 int 上界
  private IntPair ip6 = new IntPair(Integer.MAX_VALUE, Integer.MAX_VALUE);

  
  /**
   * 逐对验证比较结果
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void testComparator() throws IOException {
    // 同一个对象与自身比较，必然相等
    check(ip1, ip1, 0);
    // 第一分量 1 < 2，ip1 更小
    check(ip1, ip2, -1);
    // 第一分量 1 < 11，ip3 更小
    check(ip3, ip4, -1);
    // 第一分量 2 < 11，ip2 更小
    check(ip2, ip4, -1);
    // 第一分量 1 < Integer.MAX_VALUE
    check(ip3, ip5, -1);
    // 第一分量相等，第二分量 2 < Integer.MAX_VALUE
    check(ip5, ip6, -1);
  }
  
  /**
   * 用 {@code IntPair} 自身的默认比较器做一次比较
   *
   * @param ip1 左操作数
   * @param ip2 右操作数
   * @param c   期望的比较结果（-1 / 0 / 1）
   * @throws IOException 序列化失败
   */
  private void check(IntPair ip1, IntPair ip2, int c) throws IOException {
    // get() 会找到注册好的比较器（由 IntPair 的 static 块注册）
    check(WritableComparator.get(IntPair.class), ip1, ip2, c);
  }
  
  /**
   * 双向验证比较器：正序一次，逆序一次
   *
   * @param comp 比较器
   * @param ip1  左操作数
   * @param ip2  右操作数
   * @param c    期望的比较结果
   * @throws IOException 序列化失败
   */
  private void check(RawComparator comp, IntPair ip1, IntPair ip2, int c) throws IOException {
    checkOnce(comp, ip1, ip2, c);
    // 交换操作数，结果应当取反——这是比较器必须满足的对称性
    checkOnce(comp, ip2, ip1, -c);
  }

  /**
   * 分别用「对象级比较」与「字节级比较」验证同一个比较器
   *
   * <p>对象级比较会先把双方反序列化成对象；
   * 字节级比较则直接在序列化后的字节上比，更快，
   * 也是 Shuffle 阶段真正走的那条路
   *
   * @param comp 比较器
   * @param ip1  左操作数
   * @param ip2  右操作数
   * @param c    期望的比较结果
   * @throws IOException 序列化失败
   */
  private void checkOnce(RawComparator comp, IntPair ip1, IntPair ip2, int c) throws IOException {
    // "Object" 是断言失败时的说明文字，标明走的是对象级比较
    assertThat("Object", signum(comp.compare(ip1, ip2)), is(c));
    // 序列化两份数据，供字节级比较使用
    byte[] out1 = serialize(ip1);
    byte[] out2 = serialize(ip2);
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
