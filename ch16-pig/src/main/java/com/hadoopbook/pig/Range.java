package com.hadoopbook.pig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Range表示一个列范围，用于描述从哪一列到哪一列的提取范围。
 *
 * <p>该类是不可变的（immutable），一旦创建，其start和end属性就不能改变。
 * 主要用于CutLoadFunc中解析和表示要提取的列范围。</p>
 *
 * <p>范围说明：
 * <ul>
 *   <li>索引是<b>1-based</b>（即第一列的索引是1，不是0）</li>
 *   <li>范围是<b>闭区间</b> [start, end]，包括start和end两端点</li>
 *   <li>例如 Range(1, 3) 表示提取第1、2、3列（共3列）</li>
 * </ul>
 * </p>
 *
 * <p>使用示例：
 * <pre>
 * Range range = new Range(1, 3);
 * String line = "abcdef";
 * String extracted = range.getSubstring(line);  // 结果为 "abc"
 * </pre>
 *
 * <p>Range类还提供了静态工厂方法parse()，用于从字符串解析多个范围：
 * <pre>
 * List&lt;Range&gt; ranges = Range.parse("1-3,5-10");
 * // 解析结果：Range(1,3) 和 Range(5,10)
 * </pre>
 *
 * @see CutLoadFunc
 */
public class Range {
    /** 范围的起始列（1-based，包含start） */
    private final int start;

    /** 范围的结束列（1-based，包含end） */
    private final int end;

    /**
     * 构造一个Range实例。
     *
     * @param start 起始列索引（1-based，包含）
     * @param end   结束列索引（1-based，包含）
     * @throws IllegalArgumentException 如果start大于end或start小于1
     */
    public Range(int start, int end) {
        this.start = start;
        this.end = end;
    }

    /**
     * 获取范围的起始列索引。
     *
     * @return 起始列索引（1-based）
     */
    public int getStart() {
        return start;
    }

    /**
     * 获取范围的结束列索引。
     *
     * @return 结束列索引（1-based）
     */
    public int getEnd() {
        return end;
    }

    /**
     * 从给定字符串中提取该范围对应的子串。
     *
     * <p>由于Range使用1-based索引，而String.substring使用0-based索引，
     * 所以内部实现会将start减1转换为0-based起始位置。</p>
     *
     * <p>例如：
     * <ul>
     *   <li>Range(1, 3).getSubstring("abcdef") 返回 "abc"（第1-3列）</li>
     *   <li>Range(2, 4).getSubstring("abcdef") 返回 "bcd"（第2-4列）</li>
     * </ul>
     * </p>
     *
     * @param line 原始字符串（完整的一行数据）
     * @return 该范围对应的子串
     * @throws StringIndexOutOfBoundsException 如果end超出line长度
     */
    public String getSubstring(String line) {
        // 转换为0-based索引：start-1是起始位置，end是结束位置（exclusive）
        return line.substring(start - 1, end);
    }

    @Override
    public int hashCode() {
        // 使用与start和end相关的哈希计算公式
        // 37是一个常用的质数，用于哈希值的分散
        return start * 37 + end;
    }

    @Override
    public boolean equals(Object obj) {
        // 检查是否是同一对象
        if (!(obj instanceof Range)) {
            return false;
        }
        // 比较start和end是否相等
        Range other = (Range) obj;
        return this.start == other.start && this.end == other.end;
    }

    /**
     * 解析范围规范字符串，返回Range列表。
     *
     * <p>格式说明：
     * <ul>
     *   <li>单个个范围：start-end，例如 "1-3"</li>
     *   <li>多个范围：用逗号分隔，例如 "1-3,5-10,15-20"</li>
     * </ul>
     * </p>
     *
     * <p>解析规则：
     * <ol>
     *   <li>首先按逗号分割字符串得到各个范围规范</li>
     *   <li>然后按连字符分割得到start和end</li>
     *   <li>将start和end转换为整数并创建Range对象</li>
     * </ol>
     * </p>
     *
     * <p>异常处理：
     * <ul>
     *   <li>空字符串返回空列表</li>
     *   <li>格式无效抛出IllegalArgumentException</li>
     * </ul>
     * </p>
     *
     * <p>使用示例：
     * <pre>
     * Range.parse("")       返回 []
     * Range.parse("1-3")    返回 [Range(1,3)]
     * Range.parse("1-3,5-8")返回 [Range(1,3), Range(5,8)]
     * </pre>
     *
     * @param rangeSpec 范围规范字符串
     * @return Range对象的列表
     * @throws IllegalArgumentException 如果格式无效（如"1-nb"）
     */
    public static List<Range> parse(String rangeSpec)
        throws IllegalArgumentException {
        // 空字符串返回空列表
        if (rangeSpec.length() == 0) {
            return Collections.emptyList();
        }
        List<Range> ranges = new ArrayList<Range>();
        // 按逗号分割得到各个范围规范
        String[] specs = rangeSpec.split(",");
        for (String spec : specs) {
            // 按连字符分割得到start和end
            String[] split = spec.split("-");
            try {
                // 将字符串转换为整数并创建Range对象
                ranges.add(new Range(Integer.parseInt(split[0]), Integer
                    .parseInt(split[1])));
            } catch (NumberFormatException e) {
                // 格式无效，抛出IllegalArgumentException
                throw new IllegalArgumentException(e.getMessage());
            }
        }
        return ranges;
    }

}
