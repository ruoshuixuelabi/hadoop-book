// == IntPairTest
// cc IntPairTest 测试 IntPair 的序列化和比较功能

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.junit.Test;

/**
 * IntPairTest - IntPair 的单元测试类
 *
 * <p>此类测试 IntPair 的WritableComparable 实现,包括:</p>
 * <ul>
 *   <li>序列化/反序列化正确性</li>
 *   <li>对象比较(compareTo 方法)</li>
 *   <li>原始字节比较(RawComparator)</li>
 *   <li>边界条件测试(如 MAX_VALUE)</li>
 * </ul>
 *
 * <p><b>测试策略:</b></p>
 * <ul>
 *   <li>同时测试对象级别比较和字节级别比较</li>
 *   <li>验证 compare(a,b) = -compare(b,a) 的对称性</li>
 *   <li>测试边界值(Integer.MAX_VALUE)</li>
 * </ul>
 *
 * @see IntPair
 * @see WritableTestBase
 */
public class IntPairTest extends WritableTestBase {

    /** 测试用例: 较小的一对 */
    private IntPair ip1 = new IntPair(1, 2);

    /** 测试用例: 交换顺序后 */
    private IntPair ip2 = new IntPair(2, 1);

    /** 测试用例: first 相同,second 不同 */
    private IntPair ip3 = new IntPair(1, 12);

    /** 测试用例: first 不同 */
    private IntPair ip4 = new IntPair(11, 2);

    /** 测试用例: first 为最大整数 */
    private IntPair ip5 = new IntPair(Integer.MAX_VALUE, 2);

    /** 测试用例: 两者都为最大整数 */
    private IntPair ip6 = new IntPair(Integer.MAX_VALUE, Integer.MAX_VALUE);

    /**
     * testComparator - 测试 IntPair 的比较器
     *
     * <p>验证以下比较逻辑:</p>
     * <ul>
     *   <li>ip1 vs ip1: 相等,返回 0</li>
     *   <li>ip1 vs ip2: ip1.first < ip2.first,返回 -1</li>
     *   <li>ip3 vs ip4: ip3.first(1) < ip4.first(11),返回 -1</li>
     *   <li>ip2 vs ip4: ip2.first(2) < ip4.first(11),返回 -1</li>
     *   <li>ip3 vs ip5: ip3.first(1) < ip5.first(MAX),返回 -1</li>
     *   <li>ip5 vs ip6: ip5.second(2) < ip6.second(MAX),返回 -1</li>
     * </ul>
     *
     * <p><b>验证方法:</b></p>
     * <ol>
     *   <li>获取 IntPair 的 WritableComparator</li>
     *   <li>比较对象(调用 compare(Writable,Writable))</li>
     *   <li>比较字节数组(调用 compare(byte[],...))</li>
     *   <li>验证对称性: compare(a,b) = -compare(b,a)</li>
     * </ol>
     *
     * @throws IOException 如果序列化发生错误
     */
    @Test
    public void testComparator() throws IOException {
        // 逐对测试,验证 compareTo 的语义
        check(ip1, ip1, 0);  // 相等比较
        check(ip1, ip2, -1); // 1,2 vs 2,1: first 不同
        check(ip3, ip4, -1); // 1,12 vs 11,2: first 不同
        check(ip2, ip4, -1); // 2,1 vs 11,2: first 不同
        check(ip3, ip5, -1); // 1,12 vs MAX,2: first 不同
        check(ip5, ip6, -1); // MAX,2 vs MAX,MAX: first 相同,second 不同
    }

    /**
     * check - 使用默认比较器验证两个 IntPair 的比较结果
     *
     * @param ip1 第一个 IntPair
     * @param ip2 第二个 IntPair
     * @param c 期望的比较结果(符号)
     * @throws IOException 如果发生 I/O 错误
     */
    private void check(IntPair ip1, IntPair ip2, int c) throws IOException {
        // 获取 IntPair 的 WritableComparator
        // 这会返回 IntPair 注册的比较器(或者默认的 RawComparator)
        check(WritableComparator.get(IntPair.class), ip1, ip2, c);
    }

    /**
     * check - 使用指定 RawComparator 验证两个 IntPair 的比较结果
     *
     * <p>此方法同时验证:</p>
     * <ul>
     *   <li>对象级别比较: comp.compare(ip1, ip2)</li>
     *   <li>字节级别比较: comp.compare(bytes1, 0, len1, bytes2, 0, len2)</li>
     * </ul>
     *
     * @param comp 原始比较器
     * @param ip1 第一个 IntPair
     * @param ip2 第二个 IntPair
     * @param c 期望的比较结果符号
     * @throws IOException 如果发生 I/O 错误
     */
    private void check(RawComparator comp, IntPair ip1, IntPair ip2, int c) throws IOException {
        // 验证正向比较和反向比较
        checkOnce(comp, ip1, ip2, c);
        checkOnce(comp, ip2, ip1, -c);
    }

    /**
     * checkOnce - 执行一次完整的比较验证
     *
     * <p><b>验证步骤:</b></p>
     * <ol>
     *   <li>序列化两个 IntPair 为字节数组</li>
     *   <li>使用 RawComparator 比较字节数组</li>
     *   <li>验证比较结果的符号与期望一致</li>
     * </ol>
     *
     * @param comp RawComparator
     * @param ip1 第一个 IntPair
     * @param ip2 第二个 IntPair
     * @param c 期望的比较结果符号
     * @throws IOException 如果发生 I/O 错误
     */
    private void checkOnce(RawComparator comp, IntPair ip1, IntPair ip2, int c) throws IOException {
        // 验证对象级别比较结果的符号
        assertThat("Object", signum(comp.compare(ip1, ip2)), is(c));

        // 序列化两个对象为字节数组
        byte[] out1 = serialize(ip1);
        byte[] out2 = serialize(ip2);

        // 验证字节级别比较结果的符号
        // RawComparator 直接在字节层面进行比较,无需反序列化
        assertThat("Raw", signum(comp.compare(out1, 0, out1.length, out2, 0, out2.length)), is(c));
    }

    /**
     * signum - 返回整数的符号
     *
     * <p>将比较结果简化为 -1, 0, 1,便于断言验证。</p>
     *
     * @param i 整数
     * @return 负数为 -1,零为 0,正数为 1
     */
    private int signum(int i) {
        return i < 0 ? -1 : (i == 0 ? 0 : 1);
    }

}
