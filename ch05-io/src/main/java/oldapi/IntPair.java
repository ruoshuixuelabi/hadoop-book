package oldapi;
import java.io.*;

import org.apache.hadoop.io.*;

public class IntPair implements WritableComparable<IntPair> {

  /** 第一个整数 */
  private int first;

  /** 第二个整数 */
  private int second;

  /**
   * 默认构造函数 - Writable 框架要求必须有无参构造函数
   */
  public IntPair() {
  }

  /**
   * 构造函数 - 使用指定值创建 IntPair
   *
   * @param first 第一个整数值
   * @param second 第二个整数值
   */
  public IntPair(int first, int second) {
    set(first, second);
  }

  /**
   * 设置键值对的值
   *
   * @param first 第一个整数值
   * @param second 第二个整数值
   */
  public void set(int first, int second) {
    this.first = first;
    this.second = second;
  }

  /**
   * 获取第一个整数值
   *
   * @return first 字段
   */
  public int getFirst() {
    return first;
  }

  /**
   * 获取第二个整数值
   *
   * @return second 字段
   */
  public int getSecond() {
    return second;
  }

  /**
   * 序列化方法
   *
   * <p>将 first 和 second 写入 DataOutput,各占 4 字节。</p>
   *
   * @param out DataOutput 输出流
   * @throws IOException 如果发生 I/O 错误
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(first);
    out.writeInt(second);
  }

  /**
   * 反序列化方法
   *
   * @param in DataInput 输入流
   * @throws IOException 如果发生 I/O 错误
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    first = in.readInt();
    second = in.readInt();
  }

  /**
   * 计算哈希值
   *
   * <p>使用多项式哈希: first * 163 + second</p>
   *
   * @return 哈希码
   */
  @Override
  public int hashCode() {
    return first * 163 + second;
  }

  /**
   * 判断是否相等
   *
   * @param o 要比较的对象
   * @return 如果相等返回 true
   */
  @Override
  public boolean equals(Object o) {
    if (o instanceof IntPair) {
      IntPair ip = (IntPair) o;
      return first == ip.first && second == ip.second;
    }
    return false;
  }

  /**
   * 返回字符串表示 - 格式为 "first\tsecond"
   *
   * @return 字符串表示
   */
  @Override
  public String toString() {
    return first + "\t" + second;
  }

  /**
   * 比较两个 IntPair
   *
   * <p>首先比较 first,若相等则比较 second。</p>
   *
   * @param ip 要比较的 IntPair
   * @return 比较结果
   */
  @Override
  public int compareTo(IntPair ip) {
    int cmp = compare(first, ip.first);
    if (cmp != 0) {
      return cmp;
    }
    return compare(second, ip.second);
  }

  /**
   * 比较两个整数
   *
   * @param a 第一个整数
   * @param b 第二个整数
   * @return 比较结果
   */
  public static int compare(int a, int b) {
    return (a < b ? -1 : (a == b ? 0 : 1));
  }

}
