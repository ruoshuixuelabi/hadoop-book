package crunch;

import java.io.IOException;
import org.apache.crunch.PTable;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.lib.Sort;
import org.junit.Before;
import org.junit.Test;

import static crunch.PCollections.dump;
import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.tableOf;
import static org.junit.Assert.assertEquals;

/**
 * 验证 Sort 库函数能按 key 把一张表排好序
 *
 * <p>这里用 MemPipeline——纯内存实现——来做测试，
 * 不必真的起一个 MapReduce 作业，跑得快也更容易定位问题。
 * <p>另外记下两个扩展点：
 * 想按 value 排序，可以用 sortPairs 把键值对当成普通的二元组来处理，
 * 从而给 value 规定一个次序；
 * 想排序 Avro 对象，则需要先用 specific 生成带排序字段的记录，
 * 详见 Avro 那一章。
 */
public class SortTest {

  // 待排序的表：键和值都是整数，内容故意是无序的
  private PTable<Integer, Integer> a;

  /**
   * 每个测试方法开始前，准备一张内容无序的表
   */
  @Before
  public void setup() {
    // typedTableOf 用类型描述加若干行数据直接构造出一张内存表
    a = MemPipeline.typedTableOf(tableOf(ints(), ints()),
        2, 3, 1, 2, 2, 4);
  }

  /**
   * 排序之后 key 应当从小到大；同一个 key 的多条记录保持原来的相对次序
   *
   * @throws IOException 流水线执行失败
   */
  @Test
  public void testSortTableByKey() throws IOException {
    // 排序之前：key 是乱的
    assertEquals("{(2,3),(1,2),(2,4)}", dump(a));
    // Sort.sort 按 key 升序排序
    PTable<Integer, Integer> b = Sort.sort(a);
    // 排序之后：key 变成 1,2,2，同 key 的两条相对次序不变
    assertEquals("{(1,2),(2,3),(2,4)}", dump(b));
  }

  // use sortPairs to treat as pairs, so we can impose an order on the value

  // how to sort an Avro object? need to use specific to generate it with order fields
  // as discussed in the Avro chapter.

}
