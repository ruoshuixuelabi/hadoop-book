package com.hadoopbook.pig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 文本行里的一个列区间
 *
 * <p>用「起-止」表示，起止都从 1 开始计数，区间是闭的。
 * 例如 {@code 1-3} 表示第 1 到第 3 个字符
 * <p>之所以要减 1，是因为 Pig 的位置从 1 开始，
 * 而 Java 字符串的下标从 0 开始
 */
public class Range {
  // 起始位置，从 1 开始
  private final int start;
  // 结束位置，闭区间
  private final int end;

  /**
   * 构造一个列区间
   *
   * @param start 起始位置，从 1 开始
   * @param end 结束位置，闭区间
   */
  public Range(int start, int end) {
    // 起始位置
    this.start = start;
    // 结束位置
    this.end = end;
  }

  /**
   * 取起始位置
   *
   * @return 起始位置，从 1 开始
   */
  public int getStart() {
    return start;
  }

  /**
   * 取结束位置
   *
   * @return 结束位置，闭区间
   */
  public int getEnd() {
    return end;
  }
  
  /**
   * 按这个区间从一行文本里截取出一段
   *
   * @param line 整行文本
   * @return 截出来的片段
   */
  public String getSubstring(String line) {
    // Pig 的位置从 1 开始，Java 下标从 0 开始，所以起点要减 1
    return line.substring(start - 1, end);
  }
  
  /**
   * 计算哈希值
   *
   * @return 哈希值
   */
  @Override
  public int hashCode() {
    // 简单地把两个端点混在一起
    return start * 37 + end;
  }

  /**
   * 判断两个区间是否相同
   *
   * @param obj 待比较的对象
   * @return 起止都相同才相等
   */
  @Override
  public boolean equals(Object obj) {
    // 不是同一类，不相等
    if (!(obj instanceof Range)) {
      return false;
    }
    // 取出对方的起止
    Range other = (Range) obj;
    // 起止都相同才算相等
    return this.start == other.start && this.end == other.end;
  }

  /**
   * 把「起-止,起-止」这样的切分模式解析成一组区间
   *
   * @param rangeSpec 切分模式
   * @return 解析出来的区间列表
   * @throws IllegalArgumentException 模式格式不对时抛出
   */
  public static List<Range> parse(String rangeSpec)
      throws IllegalArgumentException {
    // 空字符串表示不切任何列
    if (rangeSpec.length() == 0) {
      return Collections.emptyList();
    }
    // 逐段解析出来的区间
    List<Range> ranges = new ArrayList<Range>();
    // 多个区间之间用逗号分隔
    String[] specs = rangeSpec.split(",");
    // 每个区间形如 "1-3"
    for (String spec : specs) {
      // 区间本身是 "起-止"
      String[] split = spec.split("-");
      try {
        // 转成 Range 对象
        ranges.add(new Range(Integer.parseInt(split[0]), Integer
            .parseInt(split[1])));
      } catch (NumberFormatException e) {
        // 起止位置不是数字，这里报一个说得清楚的错
        throw new IllegalArgumentException(e.getMessage());
      }
    }
    // 返回全部区间
    return ranges;
  }

}
