// cc TextPair A Writable implementation that stores a pair of Text objects
// cc TextPairComparator A RawComparator for comparing TextPair byte representations
// cc TextPairFirstComparator A custom RawComparator for comparing the first field of TextPair byte representations
// vv TextPair
import java.io.*;

import org.apache.hadoop.io.*;

/**
 * 把两个 {@link Text} 打包成一个可比较、可序列化的复合键
 *
 * <p>与 {@code IntPair} 相比，区别在于分量是字符串而非整数，
 * 所以比较时不能直接比字节，必须委托给 {@link Text.Comparator}
 * <p>典型用途是「站号 + 年月」这类复合主键。
 * 键里必须带上年份，否则「2007 年」和「2008 年」的数据会被错误地合并
 */
public class TextPair implements WritableComparable<TextPair> {

  /** 复合键的第一个分量 */
  private Text first;
  /** 复合键的第二个分量 */
  private Text second;
  
  /**
   * 构造一个两个分量都是空字符串的对象
   *
   * <p>MapReduce 框架需要无参构造器来反射创建实例，
   * 所以「先 new 出来、再调用 set 填值」是这类对象的惯用写法
   */
  public TextPair() {
    set(new Text(), new Text());
  }
  
  /**
   * 用两个字符串直接构造复合键
   *
   * <p>内部会各自包装成 {@link Text}，调用方不必自己转换
   *
   * @param first  第一个分量的字符串内容
   * @param second 第二个分量的字符串内容
   */
  public TextPair(String first, String second) {
    set(new Text(first), new Text(second));
  }
  
  /**
   * 用两个 {@link Text} 直接构造复合键
   *
   * @param first 第一个分量
   * @param second 第二个分量
   */
  public TextPair(Text first, Text second) {
    set(first, second);
  }
  
  /**
   * 一次写入两个分量
   *
   * <p>MapReduce 的 Mapper/Reducer 每处理一条记录都会复用同一个对象实例，
   * 因此务必提供这样的整体赋值方法，避免漏改其中一个分量
   *
   * @param first  新的第一个分量
   * @param second 新的第二个分量
   */
  public void set(Text first, Text second) {
    this.first = first;
    this.second = second;
  }
  
  /**
   * 取得第一个分量
   *
   * @return 第一个分量
   */
  public Text getFirst() {
    return first;
  }

  /**
   * 取得第二个分量
   *
   * @return 第二个分量
   */
  public Text getSecond() {
    return second;
  }

  /**
   * 把两个分量按顺序写入输出流
   *
   * <p>写入顺序必须与 {@link #readFields(DataInput)} 严格对应，
   * 否则读回来的数据会错位——这是自定义 Writable 最常见的 bug
   *
   * @param out 目标输出流
   * @throws IOException 写出失败
   */
  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }

  /**
   * 从输入流读回两个分量
   *
   * @param in 源输入流
   * @throws IOException 读取失败
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }
  
  /**
   * 计算散列值
   *
   * <p>用 163 作乘数是 Hadoop 里的惯用做法：
   * 它是质数，散列到桶里分布比较均匀，能减少哈希冲突
   *
   * @return 散列值
   */
  @Override
  public int hashCode() {
    return first.hashCode() * 163 + second.hashCode();
  }
  
  /**
   * 判断两个复合键是否按值相等
   *
   * <p>必须与 {@link #compareTo(TextPair)} 保持一致：
   * 相等的对象必须散列到同一个桶，否则 HashMap 一类的结构会出错
   *
   * @param o 待比较的对象
   * @return 两个对象的两个分量都相等时返回 true
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
   * 生成便于人阅读的字符串形式
   *
   * <p>用制表符分隔两个分量，这样在 MapReduce 的日志和作业统计信息里能对齐显示
   *
   * @return 以制表符分隔的两个分量
   */
  @Override
  public String toString() {
    return first + "\t" + second;
  }
  
  /**
   * 按「先第一分量、再第二分量」的顺序比较
   *
   * <p>先比第一分量，第一分量相等时才比第二分量。
   * 这样同一站的数据会排在一起，不同站之间不会交错，
   * 这正是要把站号放进键里的目的
   *
   * @param tp 待比较的复合键
   * @return 负数表示本对象较小，0 表示相等，正数表示本对象较大
   */
  @Override
  public int compareTo(TextPair tp) {
    int cmp = first.compareTo(tp.first);
    if (cmp != 0) {
      return cmp;
    }
    return second.compareTo(tp.second);
  }
  // ^^ TextPair
  
  // vv TextPairComparator
  /**
   * 比较两个 {@link TextPair} 的序列化字节表示
   *
   * <p>这是 {@link WritableComparator} 的「原始比较」版本：
   * 直接在序列化后的字节上比较，省去反序列化成对象的过程，
   * 因此在 Shuffle 排序阶段比 {@link #compareTo(TextPair)} 更快
   * <p>难点在于要先算出第一个分量的字节长度：
   * {@link Text} 用可变长整数（VInt）记录自身长度，
   * 所以必须先解出这个长度，才能定位到第二个分量的起点
   */
  public static class Comparator extends WritableComparator {
    
    /** 委托给 Text.Comparator 去比较单个 {@link Text} 分量 */
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
    
    /**
     * 构造比较器
     *
     * <p>把被比较的类型 {@link TextPair.class} 传给父类，
     * 父类才能在需要时反射创建实例做对象级比较
     */
    public Comparator() {
      super(TextPair.class);
    }

    /**
     * 比较两个序列化后的 {@link TextPair}
     *
     * @param b1  第一个对象的字节数组
     * @param s1  第一个对象在数组中的起始偏移
     * @param l1  第一个对象的长度
     * @param b2  第二个对象的字节数组
     * @param s2  第二个对象在数组中的起始偏移
     * @param l2  第二个对象的长度
     * @return 负数表示第一个对象较小，0 表示相等，正数表示较大
     */
    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      
      try {
        // 先解出第一个分量的长度：VInt 长度字节 + VInt 数值本身
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
        int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
        if (cmp != 0) {
          // 第一个分量不等，直接返回结果，不再看第二个分量
          return cmp;
        }
        // 第一个分量相等，再比第二个分量；起点要跳过已算出的 firstL
        return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                                       b2, s2 + firstL2, l2 - firstL2);
      } catch (IOException e) {
        // 比较器接口不抛受检异常，只能包成运行时异常
        throw new IllegalArgumentException(e);
      }
    }
  }

  // 把这个比较器注册为 {@link TextPair} 的默认比较器。
  // 注册之后，WritableComparator.get(TextPair.class) 就能找到正确的比较器，
  // 框架内部的排序（JobConf 里的 map 输出排序）会用到它
  static {
    WritableComparator.define(TextPair.class, new Comparator());
  }
  // ^^ TextPairComparator
  
  // vv TextPairFirstComparator
  /**
   * 只比较 {@link TextPair} 的第一个分量
   *
   * <p>适用于「只需要按站号分组、组内顺序无所谓」的场景：
   * 比较时跳过第二个分量，代价更小
   */
  public static class FirstComparator extends WritableComparator {
    
    /** 委托给 Text.Comparator 去比较单个 {@link Text} 分量 */
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
    
    /**
     * 构造比较器
     *
     * <p>把被比较的类型 {@link TextPair.class} 传给父类，
     * 父类才能在需要时反射创建实例做对象级比较
     */
    public FirstComparator() {
      super(TextPair.class);
    }

    /**
     * 比较两个序列化后的 {@link TextPair}，只看第一个分量
     *
     * @param b1  第一个对象的字节数组
     * @param s1  第一个对象在数组中的起始偏移
     * @param l1  第一个对象的长度
     * @param b2  第二个对象的字节数组
     * @param s2  第二个对象在数组中的起始偏移
     * @param l2  第二个对象的长度
     * @return 负数表示第一个对象较小，0 表示相等，正数表示较大
     */
    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      
      try {
        // 只需要第一个分量，但长度仍要先解出来才能定位比较范围
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
        // 只比较第一个分量，第二个分量直接忽略
        return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
      } catch (IOException e) {
        // 比较器接口不抛受检异常，只能包成运行时异常
        throw new IllegalArgumentException(e);
      }
    }
    
    /**
     * 对象级比较：只取第一个分量相比
     *
     * @param a 第一个待比较对象
     * @param b 第二个待比较对象
     * @return 负数表示第一个对象较小，0 表示相等，正数表示较大
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      if (a instanceof TextPair && b instanceof TextPair) {
        // 对象级比较：只取第一个分量比，第二个分量直接忽略
        return ((TextPair) a).first.compareTo(((TextPair) b).first);
      }
      // 类型不符时交回父类按默认规则处理
      return super.compare(a, b);
    }
  }
  // ^^ TextPairFirstComparator
  
// vv TextPair
}
// ^^ TextPair
