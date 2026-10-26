package crunch;

import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.FilterFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.junit.Rule;
import org.junit.Test;

import static crunch.PCollections.dump;
import static org.apache.crunch.types.writable.Writables.doubles;
import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Crunch 基本集合操作（Primitive Operations）的测试
 *
 * <p>本测试类覆盖 {@code PCollection} 与 {@code PTable} 上最常用的那一批操作：
 * {@code union} 合并集合、{@code parallelDo} 逐元素加工、{@code filter} 过滤、
 * {@code by} 把集合转成表、{@code groupByKey} 分组、{@code combineValues} 归并、
 * 以及 {@code length}、{@code min}、{@code max}、{@code count} 等统计动作。
 *
 * <p>断言统一借助 {@code PCollections.dump}：它把集合渲染成
 * {@code {a,b,c}}、{@code {(k,v),(k,v)}} 这样的字符串，
 * 这样既能逐项比对，又能在断言失败时一眼看出差异。
 */
public class PrimitiveOperationsTest implements Serializable {

  /**
   * 测试期间使用的临时目录，测试结束后会被自动清理
   *
   * <p>{@code transient} 表示这个字段不参与序列化：它只是 JUnit 的运行环境对象，
   * 只在测试进程内有意义，不能随作业被序列化到 MapReduce 任务里去。
   */
  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  /**
   * 用 {@code union} 把两个集合合并成一个
   *
   * <p>{@code union} 只做拼接：既不去重也不排序，
   * 结果里先是 {@code a} 的全部元素，再接着 {@code b} 的全部元素。
   *
   * @throws Exception 内存管道执行失败
   */
  @Test
  public void testPCollectionUnion() throws Exception {
    PCollection<Integer> a = MemPipeline.collectionOf(1, 3);
    PCollection<Integer> b = MemPipeline.collectionOf(2);
    // union 不改变元素顺序，也不去掉重复元素
    PCollection<Integer> c = a.union(b);
    assertEquals("{1,3,2}", dump(c));
  }

  /**
   * 用 {@code DoFn} 逐元素加工集合
   *
   * <p>{@code DoFn} 的写法是「给我一个元素，我 emit 若干个结果」，
   * 因此它天然支持一对多（展开）与一对零（过滤掉）两种情形。
   * 第二个参数 {@code ints()} 声明输出集合的类型，
   * Crunch 需要靠它来生成对应的序列化器。
   *
   * @throws Exception 内存管道执行失败
   */
  @Test
  public void testPCollectionParallelDo() throws Exception {
    PCollection<String> a = MemPipeline.collectionOf("cherry", "apple", "banana");
    // parallelDo 是惰性的：这里只是描述「每个字符串变成它的长度」，还没有真正计算
    PCollection<Integer> b = a.parallelDo(new DoFn<String, Integer>() {
      @Override
      public void process(String input, Emitter<Integer> emitter) {
        // emitter.emit 一次就多出一个结果元素；这里每个输入只 emit 一次
        emitter.emit(input.length());
      }
    // 第二个参数 ints() 声明输出集合用 Writable 的 int 类型
    }, ints());
    assertEquals("{6,5,6}", dump(b));
  }

  /**
   * 用 {@code MapFn} 逐元素加工集合
   *
   * <p>{@code MapFn} 是 {@code DoFn} 的一对一特例：返回什么就 emit 什么，
   * 每个输入元素恰好对应一个输出元素。与上一个用例结果相同，
   * 但表达式更短，语义也更直白——所以能用 {@code MapFn} 时优先用它。
   *
   * @throws Exception 内存管道执行失败
   */
  @Test
  public void testPCollectionParallelDoMap() throws Exception {
    PCollection<String> a = MemPipeline.collectionOf("cherry", "apple", "banana");
    // MapFn 只做一对一映射，因此不需要自己管理 Emitter
    PCollection<Integer> b = a.parallelDo(new MapFn<String, Integer>() {
      @Override
      public Integer map(String input) {
        // map 的返回值直接成为输出元素，这里取字符串长度
        return input.length();
      }
    }, ints());
    assertEquals("{6,5,6}", dump(b));
  }

  /**
   * 用 {@code filter} 挑出满足条件的元素
   *
   * <p>{@code FilterFn.accept} 返回 {@code true} 表示这个元素要留下，
   * 返回 {@code false} 就被丢弃。框架内部会把 {@code filter} 融合进前一个阶段，
   * 所以不会多出一个额外的 MapReduce 阶段。
   *
   * @throws Exception 内存管道执行失败
   */
  @Test
  public void testPCollectionFilter() throws Exception {
    PCollection<String> a = MemPipeline.collectionOf("cherry", "apple", "banana");
    // filter 之后得到的仍是同类型的 PCollection，只是元素变少了
    PCollection<String> b = a.filter(new FilterFn<String>() {
      @Override
      public boolean accept(String input) {
        // 只保留长度为偶数的单词：cherry 和 banana 是 6 个字母，apple 是 5 个
        return input.length() % 2 == 0; // even
      }
    });
    assertEquals("{cherry,banana}", dump(b));
  }

  /**
   * 用 {@code parallelDo} 从集合中「提取键」，把集合变成表
   *
   * <p>只要 {@code parallelDo} 的输出类型是 {@code Pair}，
   * 结果集合就会被当作一张 {@code PTable}：{@code Pair} 的第一分量是键，
   * 第二分量是值。第二个参数 {@code tableOf(ints(), strings())}
   * 分别描述了键与值的类型。
   *
   * @throws Exception 内存管道执行失败
   */
  @Test
  public void testPCollectionParallelDoExtractKey() throws Exception {
    PCollection<String> a = MemPipeline.collectionOf("cherry", "apple", "banana");
    // 输出类型是 Pair，Crunch 就把这条 parallelDo 当作「按键分组」的动作
    PTable<Integer, String> b = a.parallelDo(
        new DoFn<String, Pair<Integer, String>>() {
      @Override
      public void process(String input, Emitter<Pair<Integer, String>> emitter) {
        // Pair 的第一个分量是键，这里用字符串长度作为键
        emitter.emit(Pair.of(input.length(), input));
      }
    // tableOf 依次给出键类型与值类型，两个都要声明才能确定表的序列化方式
    }, tableOf(ints(), strings()));
    assertEquals("{(6,cherry),(5,apple),(6,banana)}", dump(b));
  }

  /**
   * 分组（group by key）的完整用法
   *
   * <p>这个用例把分组相关的能力串了一遍：{@code by} 先把集合转成表，
   * {@code groupByKey} 按键把值聚成组，得到 {@code PGroupedTable}；
   * 之后既可以用 {@code combineValues} 把每组的值归并成一个结果，
   * 也可以用 {@code mapValues} 对每组做一次映射，
   * 最后还可以用 {@code ungroup} 把分组结构拆回普通的表。
   *
   * <p>另外注意 {@code groupByKey} 的结果只能「消费一次」：
   * 同一个 {@code PGroupedTable} 的值迭代器用过一次就失效，
   * 所以每做一次新的操作都要重新 {@code groupByKey} 一次。
   *
   * @throws Exception 复制测试资源或作业执行失败
   */
  @Test
  public void testGrouping() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("fruit.txt");
    Pipeline pipeline = new MRPipeline(getClass());

    // 从真实文件读入文本，dump 出来的顺序与文件里的行顺序一致
    PCollection<String> a = pipeline.readTextFile(inputPath);
    assertEquals("{cherry,apple,banana}", dump(a));

    // by 把每个元素映射成一个键，于是集合变成了表
    PTable<Integer, String> b = a.by(new MapFn<String, Integer>() {
      @Override
      public Integer map(String input) {
        return input.length();
      }
    }, ints());
    assertEquals("{(6,cherry),(5,apple),(6,banana)}", dump(b));

    // groupByKey 把同一个键下的所有值聚成一组，键 6 下有 banana 与 cherry 两个值
    PGroupedTable<Integer, String> c = b.groupByKey();
    assertEquals("{(5,[apple]),(6,[banana,cherry])}", dump(c));

    // groupByKey(2) 指定并行度为 2，分组结果的输出顺序就不再固定，
    // 所以这里断言时两种顺序都算通过
    c = b.groupByKey(2);
    String dumpc = dump(c);
    assertTrue(dumpc, "{(5,[apple]),(6,[banana,cherry])}".equals(dumpc) ||
        "{(6,[banana,cherry]),(5,[apple])}".equals(dumpc));

    // 上一个分组结果已经被消费掉了，必须重新分组才能继续往下做
    c = b.groupByKey(); // since value iterator is single use

    // combineValues 用自定义的 CombineFn 把每组的多个值合并成一个值
    PTable<Integer, String> d = c.combineValues(new CombineFn<Integer, String>() {
      @Override
      public void process(Pair<Integer, Iterable<String>> input,
          Emitter<Pair<Integer, String>> emitter) {
        StringBuilder sb = new StringBuilder();
        for (Iterator i = input.second().iterator(); i.hasNext(); ) {
          sb.append(i.next());
          if (i.hasNext()) { sb.append(";"); }
        }
        // 把这一组的值用分号串起来，键保持不变
        emitter.emit(Pair.of(input.first(), sb.toString()));
      }
    });
    assertEquals("{(5,apple),(6,banana;cherry)}", dump(d));

    // 同样需要重新分组：分组结果不可重复使用
    c = b.groupByKey(); // since value iterator is single use

    // 用 Crunch 内置的聚合器做同样的事，结果与手写 CombineFn 一致
    PTable<Integer, String> e = c.combineValues(Aggregators.STRING_CONCAT(";",
      false));
    assertEquals("{(5,apple),(6,banana;cherry)}", dump(e));

    c = b.groupByKey();

    // mapValues 作用在分组结果上：输入是「某个键对应的整组值」，输出是单个值
    PTable<Integer, Integer> f = c.mapValues(new MapFn<Iterable<String>, Integer>() {
      @Override
      public Integer map(Iterable<String> input) {
        return Iterables.size(input);
      }
    }, ints());
    assertEquals("{(5,1),(6,2)}", dump(f));

    // ungroup 把分组结构拆开，还原成「一个键对一个值」的普通表
    PTable<Integer, String> g = c.ungroup();
    assertEquals("{(5,apple),(6,banana),(6,cherry)}", dump(g));

  }

  /**
   * 用 {@code collectValues} 把同一个键下的多个值收集成一个集合
   *
   * <p>普通表的结构是「一个键对一个值」，所以同一个键出现两次就会互相覆盖；
   * {@code collectValues} 把值类型换成 {@code Collection}，
   * 于是同一个键下的所有值都被收进一个列表里，数据不会丢失。
   *
   * @throws Exception 内存管道执行失败
   */
  @Test
  public void testPTableCollectValues() throws Exception {
    PCollection<String> a = MemPipeline.typedCollectionOf(strings(), "cherry", "apple", "banana");
    PTable<Integer,String> b = a.by(new MapFn<String, Integer>() {
      @Override
      public Integer map(String input) {
        return input.length();
      }
    }, ints());
    assertEquals("{(6,cherry),(5,apple),(6,banana)}", dump(b));

    // 键 6 下有 cherry 和 banana 两个值，它们被收集进同一个列表
    PTable<Integer, Collection<String>> c = b.collectValues();
    assertEquals("{(5,[apple]),(6,[cherry, banana])}", dump(c));
  }

  /**
   * 集合上的统计动作
   *
   * <p>{@code length}、{@code min}、{@code max} 返回的是 {@code PObject}，
   * 需要调用 {@code getValue()} 才能拿到具体数值；
   * {@code count} 返回的是一张「元素 → 出现次数」的表，
   * {@code top} 与 {@code bottom} 则在这张表上取排名靠前／靠后的若干项。
   *
   * @throws Exception 内存管道执行失败
   */
  @Test
  public void testPCollectionStats() throws Exception {
    PCollection<String> a = MemPipeline.typedCollectionOf(strings(),
        "cherry", "apple", "banana", "banana");

    // 这三个都是 PObject，getValue() 才会真正触发计算并取回结果
    assertEquals((Long) 4L, a.length().getValue());
    assertEquals("apple", a.min().getValue());
    assertEquals("cherry", a.max().getValue());

    // count 统计每个元素出现了多少次，banana 出现了两次
    PTable<String, Long> b = a.count();
    assertEquals("{(apple,1),(banana,2),(cherry,1)}", dump(b));

    // top(1) 取次数最多的那一项
    PTable<String, Long> c = b.top(1);
    assertEquals("{(banana,2)}", dump(c));

    // bottom(2) 取次数最少的两项
    PTable<String, Long> d = b.bottom(2);
    assertEquals("{(apple,1),(cherry,1)}", dump(d));
  }

  /**
   * 表与集合之间的转换，以及键值分别变换
   *
   * <p>{@code keys} 与 {@code values} 把表拆成键集合与值集合；
   * {@code mapKeys} 与 {@code mapValues} 则分别对键和值做一次映射，
   * 映射后键或值的类型会随之改变（例如整数键变成 double 键）。
   *
   * @throws Exception 内存管道执行失败
   */
  @Test
  public void testPTable() throws Exception {
    PCollection<String> a = MemPipeline.typedCollectionOf(strings(), "cherry", "apple",
        "banana");
    PTable<Integer,String> b = a.by(new MapFn<String, Integer>() {
      @Override
      public Integer map(String input) {
        return input.length();
      }
    }, ints());
    assertEquals("{(6,cherry),(5,apple),(6,banana)}", dump(b));

    // keys() 只取表的键部分，结果是一个普通集合
    PCollection<Integer> c = b.keys();
    assertEquals("{6,5,6}", dump(c));

    // values() 只取表的值部分，键在这里被丢掉了
    PCollection<String> d = b.values();
    assertEquals("{cherry,apple,banana}", dump(d));

    // mapKeys 只变换键，值原样保留；第二个参数声明新的键类型
    PTable<Double, String> e = b.mapKeys(new MapFn<Integer, Double>() {
      @Override
      public Double map(Integer input) {
        return input.doubleValue();
      }
    }, doubles());
    assertEquals("{(6.0,cherry),(5.0,apple),(6.0,banana)}", dump(e));

    // mapValues 则只变换值，键保持不变
    PTable<Integer, String> f = b.mapValues(new MapFn<String, String>() {
      @Override
      public String map(String input) {
        return input.substring(0, 1);
      }
    }, strings());
    assertEquals("{(6,c),(5,a),(6,b)}", dump(f));
  }
}
