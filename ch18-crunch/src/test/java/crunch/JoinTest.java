package crunch;

import java.io.IOException;
import java.util.Collection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.lib.Cogroup;
import org.apache.crunch.lib.Join;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static crunch.PCollections.dump;
import static crunch.PCollections.invert;
import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Join操作测试
 *
 * 演示Crunch中的各种连接操作:
 * - 内连接(INNER JOIN)
 * - 左连接(LEFT JOIN)
 * - 右连接(RIGHT JOIN)
 * - 全连接(FULL JOIN)
 * - Cogroup(分组连接)
 *
 * 使用内存管道MemPipeline进行测试。
 */
public class JoinTest {

    /** 第一个测试表: 数字到物品 */
    private PTable<Integer, String> a;

    /** 第二个测试表: 物品到人(反转后为数字到人) */
    private PTable<Integer, String> b;

    /**
     * 初始化测试数据
     *
     * a: {2->Tie, 4->Coat, 3->Hat, 1->Scarf}
     * b: {2->Joe, 4->Hank, 0->Ali, 3->Eve, 2->Hank}
     */
    @BeforeEach
    public void setup() {
        // 创建表a: 数字 -> 物品
        a = MemPipeline.typedTableOf(tableOf(ints(), strings()),
            2, "Tie", 4, "Coat", 3, "Hat", 1, "Scarf");

        // 创建表b: 人 -> 数字,然后反转
        PTable<String, Integer> b0 = MemPipeline.typedTableOf(tableOf(strings(), ints()),
            "Joe", 2, "Hank", 4, "Ali", 0, "Eve", 3, "Hank", 2);
        // 反转后: 数字 -> 人
        b = invert(b0);
    }

    /**
     * 测试内连接
     *
     * 只保留两边都存在的键
     */
    @Test
    public void testInner() throws IOException {
        assertEquals("{(2,Tie),(4,Coat),(3,Hat),(1,Scarf)}", dump(a));
        assertEquals("{(2,Joe),(4,Hank),(0,Ali),(3,Eve),(2,Hank)}", dump(b));

        // 内连接: 只保留两边都存在的键
        // a和b的交集键: 2, 3, 4
        PTable<Integer, Pair<String, String>> c = Join.join(a, b);
        assertEquals("{(2,[Tie,Joe]),(2,[Tie,Hank]),(3,[Hat,Eve]),(4,[Coat,Hank])}", dump(c));
    }

    /**
     * 测试左连接
     *
     * 保留左边所有键,右边没有的值为null
     */
    @Test
    public void testLeft() throws IOException {
        PTable<Integer, Pair<String, String>> c = Join.leftJoin(a, b);
        assertEquals("{(1,[Scarf,null]),(2,[Tie,Joe]),(2,[Tie,Hank]),(3,[Hat,Eve]),(4,[Coat,Hank])}", dump(c));
    }

    /**
     * 测试右连接
     *
     * 保留右边所有键,左边没有的值为null
     */
    @Test
    public void testRight() throws IOException {
        PTable<Integer, Pair<String, String>> c = Join.rightJoin(a, b);
        assertEquals("{(0,[null,Ali]),(2,[Tie,Joe]),(2,[Tie,Hank]),(3,[Hat,Eve]),(4,[Coat,Hank])}", dump(c));
    }

    /**
     * 测试全连接
     *
     * 保留两边所有键,没有的值为null
     */
    @Test
    public void testFull() throws IOException {
        PTable<Integer, Pair<String, String>> c = Join.fullJoin(a, b);
        assertEquals("{(0,[null,Ali]),(1,[Scarf,null]),(2,[Tie,Joe]),(2,[Tie,Hank]),(3,[Hat,Eve]),(4,[Coat,Hank])}", dump(c));
    }

    /**
     * 测试Cogroup
     *
     * 与Join不同,Cogroup保留所有键的完整值列表
     */
    @Test
    public void testCogroup() throws IOException {
        PTable<Integer, Pair<Collection<String>, Collection<String>>> c = Cogroup.cogroup(a, b);
        assertEquals("{(0,[[],[Ali]]),(1,[[Scarf],[]]),(2,[[Tie],[Joe, Hank]]),(3,[[Hat],[Eve]]),(4,[[Coat],[Hank]])}", dump(c));
    }
}