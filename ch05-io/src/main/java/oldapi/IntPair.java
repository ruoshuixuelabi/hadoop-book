package oldapi;
import java.io.*;

import org.apache.hadoop.io.*;

/**
 * 把两个 int 打包成一个可比较、可序列化的复合键（旧版 MapReduce API 版）
 *
 * <p>本类与 {@code ch05-io/src/main/java/IntPair.java} 逐行对应，
 * 区别只在包名：这里的类属于 {@code oldapi} 包，
 * 供 Hadoop 1.x 时代的 {@code org.apache.hadoop.mapred} 旧 API 使用
 * <p>MapReduce 的 key 必须是 {@link org.apache.hadoop.io.WritableComparable}：
 * 既能被序列化成字节（{@link org.apache.hadoop.io.Writable}），
 * 又能按值排序（{@link Comparable}），Shuffle 阶段才能把相同 key 的记录归到一组
 * <p>典型用途是「年月 + 温度」这类复合主键。
 * 键里必须带上年份，否则「2007 年 1 月」和「2008 年 1 月」的数据会被错误地合并
 */
public class IntPair implements WritableComparable<IntPair> {

  /** 复合键的第一个分量 */
  private int first;
  /** 复合键的第二个分量 */
  private int second;
  
  /**
   * 构造一个分量均为 0 的空对象
   *
   * <p>MapReduce 框架需要无参构造器来反射创建实例，
   * 所以「先 new 出来、再调用 set 填值」是这类对象的惯用写法
   */
  public IntPair() {
  }
  
  /**
   * 用两个分量直接构造复合键
   *
   * @param first  第一个分量
   * @param second 第二个分量
   */
  public IntPair(int first, int second) {
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
  public void set(int first, int second) {
    this.first = first;
    this.second = second;
  }
  
  /**
   * 取得第一个分量
   *
   * @return 第一个分量
   */
  public int getFirst() {
    return first;
  }

  /**
   * 取得第二个分量
   *
   * @return 第二个分量
   */
  public int getSecond() {
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
    out.writeInt(first);
    out.writeInt(second);
  }

  /**
   * 从输入流读回两个分量
   *
   * @param in 源输入流
   * @throws IOException 读取失败
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    first = in.readInt();
    second = in.readInt();
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
    return first * 163 + second;
  }
  
  /**
   * 判断两个复合键是否按值相等
   *
   * <p>必须与 {@link #compareTo(IntPair)} 保持一致：
   * 相等的对象必须散列到同一个桶，否则 HashMap 一类的结构会出错
   *
   * @param o 待比较的对象
   * @return 两个对象的两个分量都相等时返回 true
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
   * 这样同一年份的数据会排在一起，不同年份之间不会交错，
   * 这正是要把年份放进键里的目的
   *
   * @param ip 待比较的复合键
   * @return 负数表示本对象较小，0 表示相等，正数表示本对象较大
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
   * Convenience method for comparing two ints.
   *
   * <p>这里不用 {@code Integer.compare}，因为本例要兼容 Java 6；
   * 统一返回 -1/0/1 三种取值，也与 {@link Comparable} 的约定一致
   *
   * @param a 左操作数
   * @param b 右操作数
   * @return a 小于 b 返回 -1，相等返回 0，大于返回 1
   */
  public static int compare(int a, int b) {
    return (a < b ? -1 : (a == b ? 0 : 1));
  }
  
}
