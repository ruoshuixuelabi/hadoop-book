package crunch;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.crunch.types.writable.Writables.strings;
import static org.junit.Assert.assertEquals;

/**
 * 验证 Crunch / Hadoop 的「对象复用（object reuse）」现象及其正确处理方式
 *
 * <p>Hadoop 的 Reducer 有一个著名的优化：传给 reduce 的 key、value 对象会被复用，
 * 同一个对象实例在每次迭代时被就地改写成新的内容。{@code DoFn} 的文档里专门提醒过，
 * 想在迭代之外保留对象的地方，应用自己克隆一份。
 *
 * <p>Crunch 提供的标准解法是 {@code PType.getDetachedValue()}：
 * 它借助 Avro 把对象「序列化再反序列化」一遍，得到内容相同但互不影响的深拷贝。
 *
 * <p>本测试正是「把一组记录收进集合」这种典型场景——这种场景必须先脱钩，
 * 否则集合里保存的只是同一个被复用对象的引用，内容随时会被框架改写，
 * 去重结果也就不可信了。
 */
public class ObjectReuseTest implements Serializable {

  /**
   * 被测试的普通 Java 对象（POJO）
   *
   * <p>值类型用 Avro 的反射模式序列化，因此它必须提供一个公开的无参构造方法，
   * 否则 Avro 反序列化时无法创建实例；同时 {@code equals} 与 {@code hashCode}
   * 要按内容实现，收进 {@code HashSet} 时才能正确去重。
   */
  public static class StringWrapper {

    /**
     * 被包装的字符串
     */
    private String value;

    /**
     * Avro 反序列化时使用的无参构造方法，必须是 public 的
     */
    public StringWrapper() {
    }

    /**
     * 便于测试代码直接构造实例
     *
     * @param value 要包装的字符串
     */
    public StringWrapper(String value) {
      this.value = value;
    }

    /**
     * 按被包装的字符串内容判断两个对象是否相同
     *
     * <p>集合去重靠的就是它：不实现它，{@code HashSet} 就只能按引用比较，
     * 内容相同的两条记录会被当成两条。
     *
     * @param o 要比较的对象
     * @return 内容相同返回 true
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      StringWrapper myRecord = (StringWrapper) o;
      if (value != null ? !value.equals(myRecord.value) : myRecord.value != null) {
        return false;
      }
      return true;
    }

    /**
     * 与 {@link #equals(Object)} 保持一致，用被包装字符串的散列值
     *
     * @return 散列值
     */
    @Override
    public int hashCode() {
      return value != null ? value.hashCode() : 0;
    }

    /**
     * 让断言失败时的信息能直接看出记录内容
     *
     * @return 被包装的字符串
     */
    @Override public String toString() {
      return value;
    }
  }

  /**
   * 临时目录规则：测试方法执行前自动建好临时目录，执行后自动删除
   *
   * <p>标成 {@code transient}，是因为本类实现了 {@code Serializable}，
   * 而 {@code TemporaryPath} 只在本机有效，序列化它没有意义。
   */
  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  /**
   * 跑一条「分组后把每组的值收进集合」的流水线
   *
   * <p>输入文件 set2.txt 的内容是 b、c、a、e、b，其中 b 出现了两次。
   * 按单词分组之后，键 b 这一组里就有两条记录，去重之后应当只剩 1 个。
   *
   * @throws IOException 复制资源文件或执行流水线失败时抛出
   */
  @Test
  public void test() throws IOException {
    // 复制输入文件到临时目录
    String inputPath = tmpDir.copyResourceFileName("set2.txt");

    // MRPipeline 是基于 Hadoop MapReduce 的流水线实现
    Pipeline pipeline = new MRPipeline(getClass());
    // 读入文本，这一步只是把输入挂到 DAG 上，还不会启动作业
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    // 把每行包装成 StringWrapper，连同原文一起输出。
    // Avros.records 让值走 Avro 反射模式序列化，也正因为值被序列化过，
    // 后面才需要认真对待「对象复用」这件事
    PTable<String, StringWrapper> table = lines.parallelDo(new DoFn<String, Pair<String,
        StringWrapper>>() {
      @Override
      public void process(String input, Emitter<Pair<String, StringWrapper>> emitter) {
        emitter.emit(Pair.of(input, new StringWrapper(input)));
      }
    }, Avros.tableOf(Avros.strings(), Avros.records(StringWrapper.class)));
    // 分组后把每组的值去重收集起来，这一步才是本测试的重点
    PTable<String, Collection<StringWrapper>> uniques = uniqueValues(table);

    // materializeToMap() 把结果拉回本地进程并整理成一个 Map
    Map<String, Collection<StringWrapper>> materialisedUniques = uniques.materializeToMap();
    // 键 b 在输入里出现了两次，但两条记录内容相同，去重之后只剩 1 个
    assertEquals(1, materialisedUniques.get("b").size());

    // 显式跑一次流水线，确保作业已经提交并结束
    pipeline.run();
  }

  /**
   * 把一张表按键分组，并把每组里的值去重收集成一个集合
   *
   * <p>这里必须用 {@code PType.getDetachedValue()} 取出副本再放进集合：
   * 框架会复用传给 reduce 的对象，若直接保存原对象的引用，
   * 集合里的内容会在之后被框架改写，去重结果也就不可信了。
   *
   * <p>{@code getDetachedValue()} 要求对应的 {@code PType} 已经初始化过，
   * 所以这里覆盖 {@code initialize()}，在作业启动时用当前的 {@code Configuration}
   * 把值类型准备好。这个写法与 Crunch 库里的 {@code Aggregate.collectValues} 一致。
   *
   * @param table 待处理的表
   * @param <K> 键的类型
   * @param <V> 值的类型
   * @return 键到「去重后的值集合」的映射表
   */
  public static <K, V> PTable<K, Collection<V>> uniqueValues(PTable<K, V> table) {
    // 取出这张表使用的类型族，构造输出类型时要用
    PTypeFamily tf = table.getTypeFamily();
    // 值类型要单独留一份，因为下面那个匿名类里要用它做脱钩
    final PType<V> valueType = table.getValueType();
    // 先按键分组，再对每一组的值做 mapValues
    return table.groupByKey().mapValues("unique",
        new MapFn<Iterable<V>, Collection<V>>() {
          /**
           * 作业启动时初始化值类型，{@code getDetachedValue()} 依赖这一步
           */
          @Override
          public void initialize() {
            valueType.initialize(getConfiguration());
          }

          /**
           * 把同一组里的值收集成一个去重的集合
           *
           * @param values 这一组里的所有值
           * @return 去重后的集合
           */
          @Override
          public Set<V> map(Iterable<V> values) {
            Set<V> collected = new HashSet<V>();
            for (V value : values) {
              // 必须脱钩：直接放原对象的话，集合里的内容会被框架后续改写
              collected.add(valueType.getDetachedValue(value));
            }
            return collected;
          }
        }, tf.collections(table.getValueType()));
  }
}
