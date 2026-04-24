// cc IntPair 一个存储整数键值对的 WritableComparable 实现
// cc IntPairComparator 用于直接比较 IntPair 字节表示的 RawComparator

import java.io.*;

import org.apache.hadoop.io.*;

/**
 * IntPair - 实现 WritableComparable 接口的整数键值对
 *
 * <p>此类是 Hadoop Writable 框架的典型实现,用于在 MapReduce 中作为键或值使用。
 * 它存储两个整数(first 和 second),实现了序列化和反序列化方法,
 * 支持高效的数据传输和存储。</p>
 *
 * <p><b>序列化格式:</b></p>
 * <ul>
 *   <li>first: 4 字节 int 值</li>
 *   <li>second: 4 字节 int 值</li>
 * </ul>
 *
 * <p><b>排序规则:</b></p>
 * <ul>
 *   <li>首先按 first 字段升序排序</li>
 *   <li>若 first 相等,则按 second 字段升序排序</li>
 * </ul>
 *
 * <p><b>示例:</b></p>
 * <pre>{@code
 * IntPair pair = new IntPair(1, 2);
 * pair.write(out);
 * // 序列化为: [0,0,0,1][0,0,0,2]
 * }</pre>
 *
 * @see Writable
 * @see WritableComparable
 */
public class IntPair implements WritableComparable<IntPair> {

  /** 键值对的第一个整数 */
  private int first;

  /** 键值对的第二个整数 */
  private int second;

  /**
   * 默认构造函数 - Writable 框架要求必须有无参构造函数
   * 以便在反序列化时通过反射创建实例
   */
  public IntPair() {
  }

  /**
   * 构造函数 - 使用指定的两个整数创建 IntPair
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
   * @return first 字段的值
   */
  public int getFirst() {
    return first;
  }

  /**
   * 获取第二个整数值
   *
   * @return second 字段的值
   */
  public int getSecond() {
    return second;
  }

  /**
   * 序列化方法 - 将 IntPair 的状态写入 DataOutput
   *
   * <p>使用固定的 8 字节存储空间(4字节 + 4字节),
   * 先写入 first,再写入 second,均为 big-endian 格式。</p>
   *
   * @param out DataOutput 输出流
   * @throws IOException 如果发生 I/O 错误
   */
  @Override
  public void write(DataOutput out) throws IOException {
    // 使用 DataOutput 的 writeInt 方法,写入 big-endian 格式的整数
    out.writeInt(first);
    out.writeInt(second);
  }

  /**
   * 反序列化方法 - 从 DataInput 读取数据恢复 IntPair 状态
   *
   * <p>此方法必须与 write 方法严格对称,以确保序列化/反序列化的正确性。
   * 读取顺序:先 first,后 second。</p>
   *
   * @param in DataInput 输入流
   * @throws IOException 如果发生 I/O 错误
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    // 按照写入顺序读取数据
    first = in.readInt();
    second = in.readInt();
  }

  /**
   * 计算哈希值 - 用于基于哈希的数据结构(如 HashMap, HashSet)
   *
   * <p>使用简单的多项式哈希函数: first * 163 + second
   * 163 是一个质数,有助于减少哈希碰撞</p>
   *
   * @return 哈希码
   */
  @Override
  public int hashCode() {
    return first * 163 + second;
  }

  /**
   * 判断两个对象是否相等
   *
   * <p>仅当另一个对象也是 IntPair 类型,
   * 且 first 和 second 字段都相等时返回 true。</p>
   *
   * @param o 要比较的对象
   * @return 如果相等返回 true,否则返回 false
   */
  @Override
  public boolean equals(Object o) {
    if (o instanceof IntPair) {
      IntPair ip = (IntPair) o;
      // 使用 == 比较原始类型,效率更高
      return first == ip.first && second == ip.second;
    }
    return false;
  }

  /**
   * 返回字符串表示 - 格式为 "first\tsecond"
   *
   * <p>使用制表符分隔,便于调试和输出查看。</p>
   *
   * @return 字符串表示
   */
  @Override
  public String toString() {
    return first + "\t" + second;
  }

  /**
   * 比较当前 IntPair 与另一个 IntPair 的大小
   *
   * <p>实现自然顺序排序:</p>
   * <ul>
   *   <li>首先比较 first 字段</li>
   *   <li>若 first 相等,则比较 second 字段</li>
   * </ul>
   *
   * <p>此方法用于在 MapReduce 框架中对键进行排序。</p>
   *
   * @param ip 要比较的 IntPair 对象
   * @return 负数表示小于,零表示相等,正数表示大于
   */
  @Override
  public int compareTo(IntPair ip) {
    // 使用 compare 方法进行整数比较,避免直接使用 Integer.compareTo
    int cmp = compare(first, ip.first);
    if (cmp != 0) {
      return cmp;
    }
    return compare(second, ip.second);
  }

  /**
   * 比较两个整数的便捷方法
   *
   * <p>避免了 Integer.compareTo 的自动装箱开销,
   * 直接操作原始类型以提高性能。</p>
   *
   * @param a 第一个整数
   * @param b 第二个整数
   * @return 负数表示 a < b,零表示 a == b,正数表示 a > b
   */
  public static int compare(int a, int b) {
    return (a < b ? -1 : (a == b ? 0 : 1));
  }

}
