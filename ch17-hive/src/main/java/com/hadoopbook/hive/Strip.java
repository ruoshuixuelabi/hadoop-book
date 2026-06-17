package com.hadoopbook.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 去除字符串两端空白（或指定字符）的自定义函数（UDF）。
 *
 * <p>此类实现了Hive的用户自定义函数（User Defined Function），
 * 提供两个重载版本：
 * <ul>
 *   <li>{@link #evaluate(Text)} - 去除字符串两端的标准空白字符</li>
 *   <li>{@link #evaluate(Text, String)} - 去除字符串两端指定的字符集合</li>
 * </ul>
 *
 * <p>内部委托给Apache Commons Lang库的{@link StringUtils#strip(String)}方法实现，
 * 该方法基于{@link Character#isWhitespace(char)}判断空白字符。
 *
 * <p>示例Hive查询：
 * <pre>
 * CREATE TEMPORARY FUNCTION strip AS 'com.hadoopbook.hive.Strip';
 * SELECT strip('  hello  ') FROM dummy;           -- 返回 'hello'
 * SELECT strip('banana', 'ab') FROM dummy;         -- 返回 'nan'
 * </pre>
 *
 * @see UDF
 */
public class Strip extends UDF {

    /**
     * 可重用的Text对象，用于存储函数结果。
     *
     * <p>避免每次调用时创建新的Text对象，减少内存分配开销。
     * 由于Hive UDF的evaluate方法可能被频繁调用，
     * 复用同一个Text实例可以显著提升性能。
     */
    private Text result = new Text();

    /**
     * 去除字符串两端的标准空白字符。
     *
     * <p>此方法去除了Java标准空白字符
     * （空格、制表符、换行符等，详见{@link Character#isWhitespace(char)}）。
     *
     * @param str 待处理的输入字符串
     * @return 去除两端空白后的字符串；如果输入为null则返回null
     */
    public Text evaluate(Text str) {
        // null输入返回null，保持一致性
        if (str == null) {
            return null;
        }
        // 使用StringUtils.strip去除两端空白，并设置到result中
        result.set(StringUtils.strip(str.toString()));
        return result;
    }

    /**
     * 去除字符串两端指定字符集合中的字符。
     *
     * <p>此方法是{@link #evaluate(Text)}的重载版本，
     * 允许指定要去除的字符集合（stripChars参数）。
     * 当stripChars为null或空字符串时，效果等同于调用{@link #evaluate(Text)}。
     *
     * <p>例如：
     * <ul>
     *   <li>{@code strip("banana", "ab")} 返回 {@code "nan"}</li>
     *   <li>{@code strip("<<hello>>", "<>")} 返回 {@code "hello"}</li>
     * </ul>
     *
     * @param str        待处理的输入字符串
     * @param stripChars 要去除的字符集合（每个字符都会被去除）
     * @return 去除两端指定字符后的字符串；如果输入为null则返回null
     */
    public Text evaluate(Text str, String stripChars) {
        // null输入返回null，保持一致性
        if (str == null) {
            return null;
        }
        // 使用StringUtils.strip的重载版本，指定要去除的字符
        result.set(StringUtils.strip(str.toString(), stripChars));
        return result;
    }
}