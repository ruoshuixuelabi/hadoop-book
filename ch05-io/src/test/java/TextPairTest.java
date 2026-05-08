// == TextPairTest
// cc TextPairTest 测试 TextPair 的序列化和比较功能

import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertThat;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.junit.jupiter.api.Test;

/**
 * TextPairTest - TextPair 的单元测试类
 *
 * <p>测试 TextPair 的以下功能:</p>
 * <ul>
 *   <li>基本比较(compareTo 方法)</li>
 *   <li>默认 RawComparator 比较</li>
 *   <li>FirstComparator 仅比较第一个字段</li>
 *   <li>变长编码的正确处理</li>
 * </ul>
 *
 * <p><b>测试数据说明:</b></p>
 * <ul>
 *   <li>tp1-tp4: 短字符串,用于基本比较测试</li>
 *   <li>tp5-tp7: 长字符串(128字符),用于变长编码边界测试</li>
 * </ul>
 *
 * @see TextPair
 * @see TextPair.Comparator
 * @see TextPair.FirstComparator
 * @see WritableTestBase
 */
public class TextPairTest extends WritableTestBase {

    /** 测试用例: ("a", "b") */
    private TextPair tp1 = new TextPair("a", "b");

    /** 测试用例: ("b", "a") - 交换顺序 */
    private TextPair tp2 = new TextPair("b", "a");

    /** 测试用例: ("a", "ab") - first 相同,second 更长 */
    private TextPair tp3 = new TextPair("a", "ab");

    /** 测试用例: ("aa", "b") - first 更长 */
    private TextPair tp4 = new TextPair("aa", "b");

    /** 测试用例: (128个"a", "b") - 长字符串 first */
    private TextPair tp5 = new TextPair(nTimes("a", 128), "b");

    /** 测试用例: (128个"a", 128个"a") - 两者都是长字符串 */
    private TextPair tp6 = new TextPair(nTimes("a", 128), nTimes("a", 128));

    /** 测试用例: (128个"a", 128个"b") - second 不同 */
    private TextPair tp7 = new TextPair(nTimes("a", 128), nTimes("b", 128));

    /**
     * nTimes - 生成重复字符串的辅助方法
     *
     * @param s 要重复的字符
     * @param n 重复次数
     * @return n 次重复的字符串
     */
    private static String nTimes(String s, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append(s);
        }
        return sb.toString();
    }

    /**
     * testComparator - 测试 TextPair 的默认比较器
     *
     * <p>验证 TextPair.Comparator 的字节级比较功能:</p>
     * <ul>
     *   <li>tp1 vs tp1: 相等</li>
     *   <li>tp1 vs tp2: "a" < "b", 返回 -1</li>
     *   <li>tp3 vs tp4: "a" < "aa", 返回 -1</li>
     *   <li>tp2 vs tp4: "b" < "aa", 返回 1</li>
     *   <li>tp3 vs tp5: "a" < 128个"a", 返回 -1</li>
     *   <li>tp5 vs tp6: 128个"a" = 128个"a", 但 "b" < "a", 返回 1</li>
     *   <li>tp5 vs tp7: 128个"a" = 128个"a", 但 "b" < 128个"b", 返回 -1</li>
     * </ul>
     *
     * @throws IOException 如果序列化发生错误
     */
    @Test
    public void testComparator() throws IOException {
        // 基本比较测试
        check(tp1, tp1, 0);  // 相等
        check(tp1, tp2, -1); // "a" < "b"
        check(tp3, tp4, -1); // "a" < "aa"
        check(tp2, tp4, 1);   // "b" > "aa"
        check(tp3, tp5, -1); // "a" < 128个"a"

        // 长字符串测试
        check(tp5, tp6, 1);  // first 相等, "b" < "a"(128个)
        check(tp5, tp7, -1); // first 相等, "b" < "b"(128个)
    }

    /**
     * testFirstComparator - 测试 TextPair.FirstComparator
     *
     * <p>FirstComparator 仅比较 TextPair 的第一个字段,忽略第二个字段。</p>
     *
     * <p>验证:</p>
     * <ul>
     *   <li>tp1 vs tp1: first 都是 "a",相等</li>
     *   <li>tp1 vs tp2: "a" < "b", 返回 -1</li>
     *   <li>tp3 vs tp4: "a" < "aa", 返回 -1</li>
     *   <li>tp2 vs tp4: "b" > "aa", 返回 1</li>
     *   <li>tp3 vs tp5: "a" < 128个"a", 返回 -1</li>
     *   <li>tp5 vs tp6: first 都是 128个"a",相等,返回 0</li>
     *   <li>tp5 vs tp7: first 都是 128个"a",相等,返回 0</li>
     * </ul>
     *
     * <p><b>关键测试点:</b></p>
     * <ul>
     *   <li>tp5 vs tp6 和 tp5 vs tp7 返回 0,证明 FirstComparator 忽略了 second 字段</li>
     * </ul>
     *
     * @throws IOException 如果发生 I/O 错误
     */
    @Test
    public void testFirstComparator() throws IOException {
        // 创建 FirstComparator 实例
        RawComparator comp = new TextPair.FirstComparator();

        // 基本测试
        check(comp, tp1, tp1, 0);  // "a" vs "a"
        check(comp, tp1, tp2, -1); // "a" vs "b"
        check(comp, tp3, tp4, -1); // "a" vs "aa"
        check(comp, tp2, tp4, 1);  // "b" vs "aa"
        check(comp, tp3, tp5, -1); // "a" vs 128个"a"

        // 关键测试: first 相等时,忽略 second,返回 0
        check(comp, tp5, tp6, 0);  // first 相等,忽略 second
        check(comp, tp5, tp7, 0);  // first 相等,忽略 second
    }

    /**
     * check - 使用默认比较器验证两个 TextPair 的比较结果
     *
     * @param tp1 第一个 TextPair
     * @param tp2 第二个 TextPair
     * @param c 期望的比较结果符号
     * @throws IOException 如果发生 I/O 错误
     */
    private void check(TextPair tp1, TextPair tp2, int c) throws IOException {
        // 获取 TextPair 的 WritableComparator
        check(WritableComparator.get(TextPair.class), tp1, tp2, c);
    }

    /**
     * check - 使用指定 RawComparator 验证两个 TextPair 的比较结果
     *
     * <p>同时验证对象级别和字节级别的比较。</p>
     *
     * @param comp RawComparator
     * @param tp1 第一个 TextPair
     * @param tp2 第二个 TextPair
     * @param c 期望的比较结果符号
     * @throws IOException 如果发生 I/O 错误
     */
    private void check(RawComparator comp, TextPair tp1, TextPair tp2, int c) throws IOException {
        // 验证正向和反向比较的对称性
        checkOnce(comp, tp1, tp2, c);
        checkOnce(comp, tp2, tp1, -c);
    }

    /**
     * checkOnce - 执行一次完整的比较验证
     *
     * @param comp RawComparator
     * @param tp1 第一个 TextPair
     * @param tp2 第二个 TextPair
     * @param c 期望的比较结果符号
     * @throws IOException 如果发生 I/O 错误
     */
    private void checkOnce(RawComparator comp, TextPair tp1, TextPair tp2, int c) throws IOException {
        // 验证对象比较结果
        assertThat("Object", signum(comp.compare(tp1, tp2)), is(c));

        // 序列化并验证字节比较结果
        byte[] out1 = serialize(tp1);
        byte[] out2 = serialize(tp2);
        assertThat("Raw", signum(comp.compare(out1, 0, out1.length, out2, 0, out2.length)), is(c));
    }

    /**
     * signum - 返回整数的符号
     *
     * @param i 整数
     * @return 负数为 -1,零为 0,正数为 1
     */
    private int signum(int i) {
        return i < 0 ? -1 : (i == 0 ? 0 : 1);
    }

}
