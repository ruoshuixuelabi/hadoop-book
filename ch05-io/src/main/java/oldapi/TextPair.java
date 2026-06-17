package oldapi;

import java.io.*;

import org.apache.hadoop.io.*;

public class TextPair implements WritableComparable<TextPair> {

  /** 第一个 Text 对象 */
  private Text first;

  /** 第二个 Text 对象 */
  private Text second;

  /**
   * 默认构造函数 - Writable 框架要求
   */
  public TextPair() {
    set(new Text(), new Text());
  }

  /**
   * 构造函数 - 使用字符串创建 TextPair
   *
   * @param first 第一个字符串
   * @param second 第二个字符串
   */
  public TextPair(String first, String second) {
    set(new Text(first), new Text(second));
  }

  /**
   * 构造函数 - 使用 Text 对象创建 TextPair
   *
   * @param first 第一个 Text 对象
   * @param second 第二个 Text 对象
   */
  public TextPair(Text first, Text second) {
    set(first, second);
  }

  /**
   * 设置键值对的值
   *
   * @param first 第一个 Text 对象
   * @param second 第二个 Text 对象
   */
  public void set(Text first, Text second) {
    this.first = first;
    this.second = second;
  }

  /**
   * 获取第一个 Text 对象
   *
   * @return first 字段
   */
  public Text getFirst() {
    return first;
  }

  /**
   * 获取第二个 Text 对象
   *
   * @return second 字段
   */
  public Text getSecond() {
    return second;
  }

  /**
   * 序列化方法
   *
   * @param out DataOutput 输出流
   * @throws IOException 如果发生 I/O 错误
   */
  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }

  /**
   * 反序列化方法
   *
   * @param in DataInput 输入流
   * @throws IOException 如果发生 I/O 错误
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }

  /**
   * 计算哈希值
   *
   * @return 哈希码
   */
  @Override
  public int hashCode() {
    return first.hashCode() * 163 + second.hashCode();
  }

  /**
   * 判断是否相等
   *
   * @param o 要比较的对象
   * @return 如果相等返回 true
   */
  @Override
  public boolean equals(Object o) {
    if (o instanceof TextPair) {
      TextPair tp = (TextPair) o;
      return first.equals(tp.first) && second.equals(tp.second);
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
   * 比较两个 TextPair
   *
   * <p>首先比较 first,若相等则比较 second。</p>
   *
   * @param tp 要比较的 TextPair
   * @return 比较结果
   */
  @Override
  public int compareTo(TextPair tp) {
    int cmp = first.compareTo(tp.first);
    if (cmp != 0) {
      return cmp;
    }
    return second.compareTo(tp.second);
  }

  /**
   * Comparator - 用于字节级别比较的 RawComparator
   *
   * <p>此类提供了高效的原始字节比较,避免了反序列化开销。</p>
   */
  public static class Comparator extends WritableComparator {

    /** Text 类型的字节比较器 */
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

    /**
     * 默认构造函数
     */
    public Comparator() {
      super(TextPair.class);
    }

    /**
     * 在字节级别直接比较两个 TextPair
     *
     * @param b1 第一个字节数组
     * @param s1 第一个 Text 在 b1 中的起始偏移
     * @param l1 b1 中可用的字节长度
     * @param b2 第二个字节数组
     * @param s2 第二个 Text 在 b2 中的起始偏移
     * @param l2 b2 中可用的字节长度
     * @return 比较结果
     */
    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {

      try {
        // 解析第一个 Text 的长度
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);

        // 比较第一个 Text 字段
        int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
        if (cmp != 0) {
          return cmp;
        }
        // 比较第二个 Text 字段
        return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                                       b2, s2 + firstL2, l2 - firstL2);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  /**
   * 静态初始化块 - 注册 TextPair.Comparator
   */
  static {
    WritableComparator.define(TextPair.class, new Comparator());
  }

  /**
   * FirstComparator - 仅比较 TextPair 第一个字段的比较器
   *
   * <p>用于需要按第一个字段分组或连接的场景。</p>
   */
  public static class FirstComparator extends WritableComparator {

    /** Text 类型的字节比较器 */
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

    /**
     * 默认构造函数
     */
    public FirstComparator() {
      super(TextPair.class);
    }

    /**
     * 在字节级别仅比较两个 TextPair 的第一个字段
     *
     * @param b1 第一个字节数组
     * @param s1 第一个 Text 在 b1 中的起始偏移
     * @param l1 b1 中可用的字节长度
     * @param b2 第二个字节数组
     * @param s2 第二个 Text 在 b2 中的起始偏移
     * @param l2 b2 中可用的字节长度
     * @return 比较结果
     */
    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {

      try {
        // 解析第一个 Text 的长度
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
        // 仅比较第一个字段
        return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }

    /**
     * 对象级别的第一个字段比较
     *
     * @param a 第一个 TextPair 对象
     * @param b 第二个 TextPair 对象
     * @return 比较结果
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      if (a instanceof TextPair && b instanceof TextPair) {
        return ((TextPair) a).first.compareTo(((TextPair) b).first);
      }
      return super.compare(a, b);
    }
  }
}
