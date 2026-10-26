package crunch;

import java.io.IOException;
import java.util.Collection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.lib.Cogroup;
import org.apache.crunch.lib.Join;
import org.junit.Before;
import org.junit.Test;

import static crunch.PCollections.dump;
import static crunch.PCollections.invert;
import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;
import static org.junit.Assert.assertEquals;

/**
 * 验证 Crunch 的四种连接（join）方式以及分组连接（cogroup）的语义差别
 *
 * <p>Crunch 的 {@code Join} 系列 API 底层都是同一种做法：给左表打上标记 0、
 * 给右表打上标记 1，两表求并集后按「(键, 标记)」分组，再由一个 JoinFn
 * 在每个分组内部把左、右两边的记录配对输出。
 *
 * <p>所以内连接只输出两边都有的键；左外连接保证左表每条记录都出现、
 * 右边缺的值补 {@code null}；右外连接反之；全外连接则是左右外连接的并集。
 *
 * <p>而 {@code Cogroup.cogroup} 只做分组、不做配对：同一个键下左右两边的值
 * 各自保留成一个集合，因此它天然保留了「某个键只出现在一边」的信息。
 *
 * <p>这里用 {@code MemPipeline} 在内存里跑，不需要真的启动 MapReduce 作业。
 */
public class JoinTest {

  /**
   * 左表：键是整数，值是商品名
   */
  private PTable<Integer, String> a;

  /**
   * 右表：键也是整数，值是人名
   */
  private PTable<Integer, String> b;

  /**
   * 构造两张待连接的表
   *
   * <p>左表 {@code a} 的键是整数；右表 {@code b} 则是先把「名字到整数」的
   * {@code b0} 反转成「整数到名字」得到的，这样才能和 {@code a} 按同一个键连接。
   * {@code b0} 里 {@code Hank} 出现了两次，所以 {@code b} 中键 2 对应两个值，
   * 正好用来观察一对多连接时同一个键为什么会在结果里出现多行。
   */
  @Before
  public void setup() {
    // 直接在内存里造一张「整数 -> 字符串」的表
    a = MemPipeline.typedTableOf(tableOf(ints(), strings()),
        2, "Tie", 4, "Coat", 3, "Hat", 1, "Scarf");
    // 先造一张方向相反的表：字符串 -> 整数
    PTable<String, Integer> b0 = MemPipeline.typedTableOf(tableOf(strings(), ints()),
        "Joe", 2, "Hank", 4, "Ali", 0, "Eve", 3, "Hank", 2);
    // 反转键值，让 b 和 a 的键类型一致，才能按整数连接
    b = invert(b0);
  }

  /**
   * 内连接：只保留两边都有的键
   *
   * <p>键 2 在右边有两个值（Joe、Hank），所以它被配成两行；
   * 键 1 只在左表出现、键 0 只在右表出现，内连接里都被丢掉。
   *
   * @throws IOException 物化数据失败时抛出
   */
  @Test
  public void testInner() throws IOException {
    // 先确认两张输入表本身长什么样，连接结果才有参照
    assertEquals("{(2,Tie),(4,Coat),(3,Hat),(1,Scarf)}", dump(a));
    assertEquals("{(2,Joe),(4,Hank),(0,Ali),(3,Eve),(2,Hank)}", dump(b));
    // join 就是内连接：只有键相同才输出
    PTable<Integer, Pair<String, String>> c = Join.join(a, b);
    // 结果的值类型是 Pair，dump 时被渲染成 [左边,右边] 的样子
    assertEquals("{(2,[Tie,Joe]),(2,[Tie,Hank]),(3,[Hat,Eve]),(4,[Coat,Hank])}", dump(c));
  }

  /**
   * 左外连接：左表每条记录都保留，右边缺的值补 {@code null}
   *
   * <p>键 1 只在左表出现，所以右边补 {@code null}；
   * 键 0 只在右表出现，左外连接的结果里不会出现。
   *
   * @throws IOException 物化数据失败时抛出
   */
  @Test
  public void testLeft() throws IOException {
    // leftJoin 以左表为准
    PTable<Integer, Pair<String, String>> c = Join.leftJoin(a, b);
    // 键 1 在右表没有对应记录，右边的值是 null
    assertEquals("{(1,[Scarf,null]),(2,[Tie,Joe]),(2,[Tie,Hank]),(3,[Hat,Eve]),(4,[Coat,Hank])}", dump(c));
  }

  /**
   * 右外连接：右表每条记录都保留，左边缺的值补 {@code null}
   *
   * <p>键 0 只在右表出现，所以左边补 {@code null}；
   * 键 1 只在左表出现，右外连接的结果里不会出现。
   *
   * @throws IOException 物化数据失败时抛出
   */
  @Test
  public void testRight() throws IOException {
    // rightJoin 以右表为准
    PTable<Integer, Pair<String, String>> c = Join.rightJoin(a, b);
    // 键 0 在左表没有对应记录，左边的值是 null
    assertEquals("{(0,[null,Ali]),(2,[Tie,Joe]),(2,[Tie,Hank]),(3,[Hat,Eve]),(4,[Coat,Hank])}", dump(c));
  }

  /**
   * 全外连接：左右外连接的并集，缺的那一侧都补 {@code null}
   *
   * <p>键 0 只在右表、键 1 只在左表，两条都会出现在结果里。
   *
   * @throws IOException 物化数据失败时抛出
   */
  @Test
  public void testFull() throws IOException {
    // fullJoin 把两个方向的外连接合到一起
    PTable<Integer, Pair<String, String>> c = Join.fullJoin(a, b);
    // 结果里既有左边补 null 的键 0，也有右边补 null 的键 1
    assertEquals("{(0,[null,Ali]),(1,[Scarf,null]),(2,[Tie,Joe]),(2,[Tie,Hank]),(3,[Hat,Eve]),(4,[Coat,Hank])}", dump(c));
  }

  /**
   * 分组连接：只按键分组，不在组内做左右配对
   *
   * <p>结果的每个键对应两个集合：左边一个、右边一个。
   * 所以键 2 只出现一行，值是 {@code [[Tie],[Joe, Hank]]}；
   * 键 1 是 {@code [[Scarf],[]]}，键 0 是 {@code [[],[Ali]]}，
   * 只出现在一边的键同样被保留——这正是 cogroup 与 join 最大的区别。
   *
   * @throws IOException 物化数据失败时抛出
   */
  @Test
  public void testCogroup() throws IOException {
    // cogroup 的值类型是 Pair<Collection, Collection>，两边各一个集合
    PTable<Integer, Pair<Collection<String>, Collection<String>>> c = Cogroup.cogroup(a, b);
    // 组内不做左右配对，所以同一个键只出现一次
    assertEquals("{(0,[[],[Ali]]),(1,[[Scarf],[]]),(2,[[Tie],[Joe, Hank]]),(3,[[Hat],[Eve]]),(4,[[Coat],[Hank]])}", dump(c));
  }
}
