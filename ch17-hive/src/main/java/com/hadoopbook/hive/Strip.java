package com.hadoopbook.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Hive 普通函数：去掉字符串两端的空白
 *
 * <p>{@link UDF} 一次只看一行，
 * 输入一行就输出一行，不需要跨行累计
 * <p>它提供了两个重载：
 * 一个去掉所有空白字符，一个只去掉指定的那些字符
 */
public class Strip extends UDF {
  // 复用的返回值对象：Hive 常常会反复调用同一个 UDF 实例
  private Text result = new Text();
  
  /**
   * 去掉字符串两端的空白字符
   *
   * @param str 输入字符串
   * @return 处理后的字符串；输入为 {@code null} 时返回 {@code null}
   */
  public Text evaluate(Text str) {
    // 输入为空，什么也不做
    if (str == null) {
      // 原样返回空
      return null;
    }
    // 去掉两端的所有空白字符
    result.set(StringUtils.strip(str.toString()));
    // 返回复用的那个对象
    return result;
  }
  
  /**
   * 去掉字符串两端指定的那些字符
   *
   * @param str 输入字符串
   * @param stripChars 要去掉哪些字符
   * @return 处理后的字符串；输入为 {@code null} 时返回 {@code null}
   */
  public Text evaluate(Text str, String stripChars) {
    // 输入为空，什么也不做
    if (str == null) {
      // 原样返回空
      return null;
    }
    // 只去掉两端出现在 stripChars 里的字符
    result.set(StringUtils.strip(str.toString(), stripChars));
    // 返回复用的那个对象
    return result;
  }
}