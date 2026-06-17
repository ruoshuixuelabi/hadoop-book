package com.hadoopbook.pig;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.*;

/**
 * Range类的单元测试类。
 *
 * <p>该测试类验证Range类的解析和提取功能是否正确。
 * 测试覆盖了空字符串解析、单范围解析、多范围解析以及无效格式等场景。</p>
 *
 * <p>Range类的使用场景：
 * <ul>
 *   <li>用于CutLoadFunc中指定要提取的列范围</li>
 *   <li>范围是1-based索引，闭区间[start, end]</li>
 *   <li>解析格式为"start-end[,start-end,...]"</li>
 * </ul>
 * </p>
 *
 * @see Range
 * @see CutLoadFunc
 */
public class RangeTest {

    /**
     * 测试空字符串的解析。
     *
     * <p>验证当传入空字符串""时，parse方法返回空列表。</p>
     */
    @Test
    public void parsesEmptyRangeSpec() {
      assertThat(Range.parse("").size(), is(0));
    }

    /**
     * 测试单个范围规范的解析。
     *
     * <p>验证当传入单个范围"1-3"时，parse方法返回一个包含Range(1,3)的列表。</p>
     */
    @Test
    public void parsesSingleRangeSpec() {
      List<Range> ranges = Range.parse("1-3");
      assertThat(ranges.size(), is(1));
      assertThat(ranges.get(0), is(new Range(1, 3)));
    }

    /**
     * 测试多个范围规范的解析。
     *
     * <p>验证当传入多个范围"1-3,5-10"时，parse方法返回一个包含
     * Range(1,3)和Range(5,10)的列表。</p>
     */
    @Test
    public void parsesMultipleRangeSpec() {
      List<Range> ranges = Range.parse("1-3,5-10");
      assertThat(ranges.size(), is(2));
      assertThat(ranges.get(0), is(new Range(1, 3)));
      assertThat(ranges.get(1), is(new Range(5, 10)));
    }

    /**
     * 测试无效格式规范的解析。
     *
     * <p>验证当传入无效的格式（如"1-nb"）时，parse方法抛出IllegalArgumentException。</p>
     */
    @Test(expected = IllegalArgumentException.class)
    public void failsOnInvalidSpec() {
      Range.parse("1-nb");
    }
}
