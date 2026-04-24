package com.hadoopbook.pig;

import org.apache.pig.PrimitiveEvalFunc;

//cc Trim An EvalFunc UDF to trim leading and trailing whitespace from chararray values
//vv Trim

/**
 * Trim是一个Pig PrimitiveEvalFunc UDF，用于去除字符串两端的白空格。
 *
 * <p>该类继承自PrimitiveEvalFunc，是Pig中用于处理基本类型的高效UDF基类。
 * Trim函数会去除字符串两端的空白字符，包括空格、制表符、换行符等。</p>
 *
 * <p>功能说明：
 * <ul>
 *   <li>去除字符串<b>前端</b>的空白字符</li>
 *   <li>去除字符串<b>后端</b>的空白字符</li>
 *   <li>字符串<b>中间</b>的空白字符会被保留</li>
 *   <li>如果字符串全为空白字符，返回空字符串</li>
 *   <li>null输入返回null</li>
 * </ul>
 * </p>
 *
 * <p>使用示例（Pig Latin）：
 * <pre>
 * -- 假设数据格式为: id, name, value
 * raw = LOAD 'data.txt' AS (id:int, name:chararray, value:double);
 *
 * -- 去除name字段两端的空白
 * trimmed = FOREACH raw GENERATE id, com.hadoopbook.pig.Trim(name), value;
 * </pre>
 *
 * <p>执行示例：
 * <pre>
 * Trim.exec("  hello  ") 返回 "hello"
 * Trim.exec("\t world\n") 返回 "world"
 * Trim.exec("   ")        返回 ""
 * Trim.exec(null)         返回 null
 * </pre>
 *
 * <p>该UDF是对Java标准库String.trim()方法的简单包装，
 * 利用了PrimitiveEvalFunc提供的高效类型处理机制。</p>
 *
 * @see PrimitiveEvalFunc
 */
public class Trim extends PrimitiveEvalFunc<String, String> {
    /**
     * 执行去空格操作。
     *
     * <p>此方法调用String.trim()来去除字符串两端的空白字符。</p>
     *
     * @param input 要处理的输入字符串
     * @return 去除两端空白后的字符串，如果输入为null则返回null
     */
    @Override
    public String exec(String input) {
        return input.trim();
    }
}
// ^^ Trim
