package com.hadoopbook.pig;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.*;

/**
 * {@link Range} 的区间解析测试
 *
 * <p>重点检查「起-止」这种写法能不能被正确解析，
 * 以及格式不对时会不会报错
 */
public class RangeTest {

  /**
   * 空字符串切不出任何区间
   */
  @Test
  public void parsesEmptyRangeSpec() {
    // 解析结果是空的
    assertThat(Range.parse("").size(), is(0));
  }

  /**
   * 只有一个区间时能正确解析
   */
  @Test
  public void parsesSingleRangeSpec() {
    // 一个区间
    List<Range> ranges = Range.parse("1-3");
    // 解析出一个区间
    assertThat(ranges.size(), is(1));
    // 起止分别是 1 和 3
    assertThat(ranges.get(0), is(new Range(1, 3)));
  }

  /**
   * 多个区间时能逐个解析出来
   */
  @Test
  public void parsesMultipleRangeSpec() {
    // 两个区间
    List<Range> ranges = Range.parse("1-3,5-10");
    // 解析出两个区间
    assertThat(ranges.size(), is(2));
    // 第一个区间
    assertThat(ranges.get(0), is(new Range(1, 3)));
    // 第二个区间是 5 到 10
    assertThat(ranges.get(1), is(new Range(5, 10)));
  }

  /**
   * 结束位置不是数字时要报错
   */
  @Test(expected = IllegalArgumentException.class)
  public void failsOnInvalidSpec() {
    // "n" 不是数字，应当抛出 IllegalArgumentException
    Range.parse("1-n");
  }
}
