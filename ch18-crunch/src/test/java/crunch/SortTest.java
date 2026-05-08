package crunch;

import java.io.IOException;
import org.apache.crunch.PTable;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.lib.Sort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static crunch.PCollections.dump;
import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.tableOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 排序测试
 *
 * 演示Crunch的PTable排序功能。
 * 使用内存管道MemPipeline进行测试。
 */
public class SortTest {

    /** 测试表 */
    private PTable<Integer, Integer> a;

    /**
     * 初始化测试数据
     *
     * 创建表: {(2,3),(1,2),(2,4)}
     */
    @BeforeEach
    public void setup() {
        a = MemPipeline.typedTableOf(tableOf(ints(), ints()),
            2, 3, 1, 2, 2, 4);
    }

    /**
     * 测试按键排序
     *
     * 排序结果应按键升序排列: {(1,2),(2,3),(2,4)}
     */
    @Test
    public void testSortTableByKey() throws IOException {
        // 排序前: {(2,3),(1,2),(2,4)}
        assertEquals("{(2,3),(1,2),(2,4)}", dump(a));
        // 排序后: {(1,2),(2,3),(2,4)}
        PTable<Integer, Integer> b = Sort.sort(a);
        assertEquals("{(1,2),(2,3),(2,4)}", dump(b));
    }

    // use sortPairs to treat as pairs, so we can impose an order on the value

    // how to sort an Avro object? need to use specific to generate it with order fields
    // as discussed in the Avro chapter.

}