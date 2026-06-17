// cc TextPair 一个存储一对 Text 对象的 Writable 实现
// cc TextPairComparator 用于直接比较 TextPair 字节表示的 RawComparator
// cc TextPairFirstComparator 用于仅比较 TextPair 第一个字段的 RawComparator

import java.io.*;

import org.apache.hadoop.io.*;

/**
 * TextPair - 实现 WritableComparable 接口的 Text 键值对
 *
 * <p>与 IntPair 类似,但使用 Hadoop 的 Text 类型存储字符串。
 * Text 是 UTF-8 编码的可变长度字符串,相比 Java 的 String 更节省空间。</p>
 *
 * <p><b>序列化格式:</b></p>
 * <ul>
 *   <li>first: Text 序列化格式 (变长整数长度 + UTF-8 字节)</li>
 *   <li>second: Text 序列化格式 (变长整数长度 + UTF-8 字节)</li>
 * </ul>
 *
 * <p><b>排序规则:</b></p>
 * <ul>
 *   <li>首先按 first 字段字典序排序</li>
 *   <li>若 first 相等,则按 second 字段字典序排序</li>
 * </ul>
 *
 * @see Writable
 * @see WritableComparable
 * @see Text
 */
public class TextPair implements WritableComparable<TextPair> {

    /** 键值对的第一个 Text 对象 */
    private Text first;

    /** 键值对的第二个 Text 对象 */
    private Text second;

    /**
     * 默认构造函数 - Writable 框架要求必须有无参构造函数
     */
    public TextPair() {
        set(new Text(), new Text());
    }

    /**
     * 构造函数 - 使用字符串创建 TextPair
     *
     * @param first 第一个字符串
     * @param second 第二个字符串
     */
    public TextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }

    /**
     * 构造函数 - 使用 Text 对象创建 TextPair
     *
     * @param first 第一个 Text 对象
     * @param second 第二个 Text 对象
     */
    public TextPair(Text first, Text second) {
        set(first, second);
    }

    /**
     * 设置键值对的值
     *
     * @param first 第一个 Text 对象
     * @param second 第二个 Text 对象
     */
    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    /**
     * 获取第一个 Text 对象
     *
     * @return first 字段的引用
     */
    public Text getFirst() {
        return first;
    }

    /**
     * 获取第二个 Text 对象
     *
     * @return second 字段的引用
     */
    public Text getSecond() {
        return second;
    }

    /**
     * 序列化方法 - 将 TextPair 写入 DataOutput
     *
     * <p>直接委托给 Text 对象的 write 方法,
     * Text 使用变长编码,更节省序列化的字节数。</p>
     *
     * @param out DataOutput 输出流
     * @throws IOException 如果发生 I/O 错误
     */
    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    /**
     * 反序列化方法 - 从 DataInput 读取数据恢复 TextPair 状态
     *
     * @param in DataInput 输入流
     * @throws IOException 如果发生 I/O 错误
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    /**
     * 计算哈希值
     *
     * <p>使用 Text 的哈希码,乘以质数 163 后相加。
     * Text 的哈希码基于其 UTF-8 字节内容计算。</p>
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    /**
     * 判断两个 TextPair 是否相等
     *
     * @param o 要比较的对象
     * @return 如果相等返回 true
     */
    @Override
    public boolean equals(Object o) {
        if (o instanceof TextPair) {
            TextPair tp = (TextPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    /**
     * 返回字符串表示 - 格式为 "first\tsecond"
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return first + "\t" + second;
    }

    /**
     * 比较当前 TextPair 与另一个 TextPair 的大小
     *
     * <p>Text 实现了 Comparable<Text>,直接使用其 compareTo 方法进行比较。</p>
     *
     * @param tp 要比较的 TextPair 对象
     * @return 负数表示小于,零表示相等,正数表示大于
     */
    @Override
    public int compareTo(TextPair tp) {
        int cmp = first.compareTo(tp.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(tp.second);
    }
    // ^^ TextPair

    /**
     * TextPairComparator - 直接在字节级别比较两个 TextPair 的 RawComparator 实现
     *
     * <p>此类提供了高效的原始字节比较,避免了反序列化开销。
     * 它直接从字节数组中读取变长整数长度,然后使用 Text.Comparator 进行比较。</p>
     *
     * <p><b>字节格式解析:</b></p>
     * <ul>
     *   <li>变长整数(VInt)表示长度</li>
     *   <li>后续字节为实际的 UTF-8 编码内容</li>
     * </ul>
     *
     * @see WritableComparator
     * @see org.apache.hadoop.io.Text.Comparator
     */
    // vv TextPairComparator
    public static class Comparator extends WritableComparator {

        /** Text 类型的字节比较器 */
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        /**
         * 默认构造函数 - 注册 TextPair 类
         */
        public Comparator() {
            super(TextPair.class);
        }

        /**
         * 在字节级别直接比较两个 TextPair
         *
         * <p>此方法实现了 RawComparator 接口,可以直接比较序列化的字节数组,
         * 无需先反序列化对象,显著提高排序性能。</p>
         *
         * <p><b>比较逻辑:</b></p>
         * <ol>
         *   <li>从字节数组中解析第一个 Text 的长度(变长整数)</li>
         *   <li>比较第一个 Text 的内容</li>
         *   <li>若相等,比较第二个 Text 的内容</li>
         * </ol>
         *
         * @param b1 第一个字节数组
         * @param s1 第一个 Text 在 b1 中的起始偏移
         * @param l1 b1 中可用的字节长度
         * @param b2 第二个字节数组
         * @param s2 第二个 Text 在 b2 中的起始偏移
         * @param l2 b2 中可用的字节长度
         * @return 负数表示 b1 < b2,零表示相等,正数表示大于
         */
        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                // 解析第一个 Text 的长度: 变长整数大小 + 变长整数值
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);

                // 比较第一个 Text 字段
                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                if (cmp != 0) {
                    return cmp;
                }
                // 比较第二个 Text 字段(跳过第一个 Text 的字节)
                return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                        b2, s2 + firstL2, l2 - firstL2);
            }
            catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    /**
     * 静态初始化块 - 注册 TextPair.Comparator
     *
     * <p>将自定义的 Comparator 注册到 WritableComparator,
     * 使得 WritableComparator.get(TextPair.class) 返回此比较器。</p>
     */
    static {
        WritableComparator.define(TextPair.class, new Comparator());
    }
    // ^^ TextPairComparator

    /**
     * FirstComparator - 仅比较 TextPair 第一个字段的 RawComparator
     *
     * <p>在需要按 TextPair 的第一个字段进行分组或连接时非常有用,
     * 例如在二次排序中按第一个键分组。</p>
     *
     * <p><b>用途:</b></p>
     * <ul>
     *   <li>MapReduce 中的二级排序</li>
     *   <li>按第一个字段分组进行连接操作</li>
     * </ul>
     *
     * @see WritableComparator
     */
    // vv TextPairFirstComparator
    public static class FirstComparator extends WritableComparator {

        /** Text 类型的字节比较器 */
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        /**
         * 默认构造函数
         */
        public FirstComparator() {
            super(TextPair.class);
        }

        /**
         * 在字节级别仅比较两个 TextPair 的第一个字段
         *
         * <p>此方法仅解析和比较第一个 Text,忽略第二个字段。</p>
         *
         * @param b1 第一个字节数组
         * @param s1 第一个 Text 在 b1 中的起始偏移
         * @param l1 b1 中可用的字节长度
         * @param b2 第二个字节数组
         * @param s2 第二个 Text 在 b2 中的起始偏移
         * @param l2 b2 中可用的字节长度
         * @return 负数表示 b1 < b2,零表示相等,正数表示大于
         */
        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                // 解析第一个 Text 的长度
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                // 仅比较第一个字段
                return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            }
            catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        /**
         * 对象级别的第一个字段比较
         *
         * <p>当对象已经反序列化时,使用此方法进行比较,
         * 避免重复解析字节数组。</p>
         *
         * @param a 第一个 TextPair 对象
         * @param b 第二个 TextPair 对象
         * @return 比较结果
         */
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof TextPair && b instanceof TextPair) {
                return ((TextPair) a).first.compareTo(((TextPair) b).first);
            }
            return super.compare(a, b);
        }
    }
    // ^^ TextPairFirstComparator

// vv TextPair
}
