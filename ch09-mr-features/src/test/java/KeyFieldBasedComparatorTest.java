import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertThat;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.KeyFieldBasedComparator;
import org.junit.jupiter.api.Test;

/**
 * KeyFieldBasedComparator测试类.
 *
 * 本测试验证KeyFieldBasedComparator的按键字段比较功能:
 *
 * 1. 功能说明:
 *    - KeyFieldBasedComparator是Hadoop提供的按键字段比较器
 *    - 支持类似Unix sort的-k选项语法
 *    - 可以指定按哪个字段、字段的哪个字符进行比较
 *    - 支持数值排序(-kn)和反向排序(-knr)
 *
 * 2. 选项语法:
 *    - -kPos:指定起始字段位置
 *    - -kPos1,Pos2:指定起始和结束字段
 *    - -kPos.c:指定字段内的起始字符位置
 *    - n:数值排序
 *    - r:反向(降序)排序
 *
 * 3. 测试场景:
 *    - firstKey:第一字段的各种排序方式
 *    - secondKey:第二字段的各种排序方式
 *    - firstThenSecondKey:多字段组合排序
 *
 * @see KeyFieldBasedComparator
 */
public class KeyFieldBasedComparatorTest {

    // 测试数据:两列Tab分隔的文本
    // line1: "2\t30" - 第一字段2,第二字段30
    // line2: "10\t4" - 第一字段10,第二字段4
    // line3: "10\t30" - 第一字段10,第二字段30
    Text line1 = new Text("2\t30");
    Text line2 = new Text("10\t4");
    Text line3 = new Text("10\t30");

    /**
     * 测试第一字段的各种排序方式.
     *
     * @throws Exception 测试异常
     */
    @Test
    public void firstKey() throws Exception {
        // -k1,1:只按第一字段排序,字符串比较 "2" < "10"
        check("-k1,1", line1, line2, 1);
        // -k1:等同于-k1,1
        check("-k1",   line1, line2, 1);
        // -k1.1:第一字段的第一个字符 "2" vs "1"
        check("-k1.1", line1, line2, 1);
        // -k1n:第一字段数值比较 2 < 10
        check("-k1n",  line1, line2, -1);
        // -k1nr:第一字段数值反向排序 10 > 2,故line2 > line1
        check("-k1nr", line1, line2, 1);
    }

    /**
     * 测试第二字段的各种排序方式.
     *
     * @throws Exception 测试异常
     */
    @Test
    public void secondKey() throws Exception {
        // -k2,2:只按第二字段排序,字符串比较 "30" > "4"
        check("-k2,2", line1, line2, -1);
        // -k2:等同于-k2,2
        check("-k2",   line1, line2, -1);
        // -k2.1:第二字段的第一个字符 "3" vs "4"
        check("-k2.1", line1, line2, -1);
        // -k2n:第二字段数值比较 30 > 4
        check("-k2n",  line1, line2, 1);
        // -k2nr:第二字段数值反向排序 4 < 30,故line2 < line1
        check("-k2nr", line1, line2, -1);
    }

    /**
     * 测试多字段组合排序方式.
     *
     * @throws Exception 测试异常
     */
    @Test
    public void firstThenSecondKey() throws Exception {
        // -k1 -k2:先按第一字段,第一字段相同则按第二字段
        // line1(2,30) vs line2(10,4):第一字段2<10,返回1
        check("-k1 -k2",   line1, line2, 1);
        // line2(10,4) vs line3(10,30):第一字段相同10=10,比较第二字段4<30,返回1
        check("-k1 -k2",   line2, line3, 1);
        // -k1 -k2nr:先按第一字段,第一字段相同则按第二字段数值反向排序
        // line2(10,4) vs line3(10,30):第一字段相同,数值反向30>4,line3>line2,返回1
        check("-k1 -k2nr", line2, line3, 1);
    }

    /**
     * 辅助方法:使用KeyFieldBasedComparator比较两个文本.
     *
     * @param options  比较器选项(如"-k1,1")
     * @param l1       第一个文本
     * @param l2       第二个文本
     * @param c        期望的比较结果:正数l1>l2,负数l1<l2,零相等
     * @throws IOException 测试异常
     */
    private void check(String options, Text l1, Text l2, int c) throws IOException {
        JobConf conf = new JobConf();
        // 设置比较器选项,类似Unix sort的-k选项
        conf.setKeyFieldComparatorOptions(options);
        KeyFieldBasedComparator comp = new KeyFieldBasedComparator();
        // 配置比较器,使其可以访问JobConf中的选项
        comp.configure(conf);

        // 将文本序列化为字节数组
        DataOutputBuffer out1 = serialize(l1);
        DataOutputBuffer out2 = serialize(l2);
        // 执行比较并验证结果
        assertThat(options, comp.compare(out1.getData(), 0, out1.getLength(),
            out2.getData(), 0, out2.getLength()), is(c));
    }

    /**
     * 将Writable对象序列化为DataOutputBuffer.
     *
     * @param writable 要序列化的Writable对象
     * @return 序列化后的字节缓冲区
     * @throws IOException IO异常
     */
    public static DataOutputBuffer serialize(Writable writable) throws IOException {
        DataOutputBuffer out = new DataOutputBuffer();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        return out;
    }

}