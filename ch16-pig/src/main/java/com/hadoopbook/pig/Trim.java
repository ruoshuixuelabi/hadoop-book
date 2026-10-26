package com.hadoopbook.pig;

import org.apache.pig.PrimitiveEvalFunc;

//cc Trim An EvalFunc UDF to trim leading and trailing whitespace from chararray values
//vv Trim
/**
 * 去掉字符串两端的空白
 *
 * <p>这是一个 {@code EvalFunc}：
 * 输入一个字符串，输出一个字符串
 */
public class Trim extends PrimitiveEvalFunc<String, String> {
  /**
   * 去掉输入字符串两端的空白
   *
   * @param input 输入字符串
   * @return 去掉两端空白后的字符串
   */
  @Override
  public String exec(String input) {
    // Java 的 trim 正好去掉两端空白
    return input.trim();
  }
}
// ^^ Trim