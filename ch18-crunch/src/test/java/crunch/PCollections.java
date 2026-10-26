package crunch;

import java.util.Iterator;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PTypeFamily;

import static org.apache.crunch.types.writable.Writables.strings;

/**
 * 测试辅助类：把 Crunch 的各种集合渲染成便于断言的字符串
 *
 * <p>{@code PCollection}、{@code PTable} 自带的 toString() 并不直观，
 * 直接比较很难在断言失败时看出差异，
 * 所以这里统一渲染成 {@code {a,b,c}}、{@code {(k,v),(k,v)}} 这样的固定格式，
 * 测试里就能直接用 {@code assertEquals} 比较一整段字符串。
 */
public class PCollections {

  /**
   * 把一个 PCollection 渲染成 {@code {a,b,c}} 形式
   *
   * @param collection 待渲染的集合
   * @param <S> 集合中元素的类型
   * @return 形如 {@code {a,b,c}} 的字符串
   */
  public static <S> String dump(PCollection<S> collection) {
    StringBuilder sb = new StringBuilder("{");
    // materialize() 才会真正触发计算，把结果从集群拉回本地进程
    for (Iterator<S> i = collection.materialize().iterator(); i.hasNext(); ) {
      // 逐个把元素追加进字符串
      sb.append(i.next());
      if (i.hasNext()) {
        // 只有后面还有元素时才补逗号，这样结尾不会多出一个逗号
        sb.append(",");
      }
    }
    // 补上右花括号，整个结果就是 {a,b,c}
    sb.append("}");
    return sb.toString();
  }

  /**
   * 把一张 PTable 渲染成 {@code {(k1,v1),(k2,v2)}} 形式
   *
   * @param table 待渲染的表
   * @param <K> 键的类型
   * @param <V> 值的类型
   * @return 形如 {@code {(k1,v1),(k2,v2)}} 的字符串
   */
  public static <K, V> String dump(PTable<K, V> table) {
    StringBuilder sb = new StringBuilder("{");
    // 和 PCollection 一样，materialize() 会把表的内容取回本地
    for (Iterator<Pair<K, V>> i = table.materialize().iterator(); i.hasNext(); ) {
      // 取出当前这一对键值
      Pair<K, V> pair = i.next();
      // 把键值对渲染成 (k,v) 的样子
      sb.append("(").append(pair.first()).append(",").append(pair.second()).append(")");
      if (i.hasNext()) {
        // 同样只在后面还有元素时才补逗号
        sb.append(",");
      }
    }
    // 补上右花括号，得到 {(k1,v1),(k2,v2)}
    sb.append("}");
    return sb.toString();
  }

  /**
   * 把一张 PGroupedTable 渲染成 {@code {(k1,[v1,v2]),(k2,[v3])}} 形式
   *
   * @param groupedTable 待渲染的分组表
   * @param <K> 键的类型
   * @param <V> 值的类型
   * @return 形如 {@code {(k1,[v1,v2]),(k2,[v3])}} 的字符串
   */
  public static <K, V> String dump(PGroupedTable<K, V> groupedTable) {
    // 分组表里同一个 key 对应一组值，
    // 先用 mapValues 把每组的值压成一个字符串，
    // 再交给上面那个 PTable 版本的 dump 去统一渲染
    return dump(groupedTable.mapValues(new MapFn<Iterable<V>, String>() {
      /**
       * 把同一组里的值渲染成 {@code [v1,v2]} 形式
       *
       * @param input 该 key 对应的所有值
       * @return 形如 {@code [v1,v2]} 的字符串
       */
      @Override
      public String map(Iterable<V> input) {
        StringBuilder sb = new StringBuilder();
        // 用方括号表示「这是一组值」
        sb.append("[");
        for (Iterator<V> i = input.iterator(); i.hasNext(); ) {
          // 逐个把这一组里的值追加进去
          sb.append(i.next());
          if (i.hasNext()) {
            // 组内同样不留多余的逗号
            sb.append(",");
          }
        }
        // 收尾的右方括号
        sb.append("]");
        return sb.toString();
      }
    }, strings()));
  }

  /**
   * 把一张表的键和值互换，得到「键为原值、值为原键」的新表
   *
   * @param table 原表
   * @param <K> 原表的键类型
   * @param <V> 原表的值类型
   * @return 键值互换之后的新表
   */
  public static <K, V> PTable<V, K> invert(PTable<K, V> table) {
    // 取出原表使用的类型族；键值互换之后类型也要跟着换
    PTypeFamily tf = table.getTypeFamily();
    PTableType<V, K> type = tf.tableOf(table.getValueType(), table.getKeyType());
    // 用 InversePairFn 在 map 阶段把每个 (k,v) 变成 (v,k)
    return table.parallelDo(new InversePairFn<K, V>(), type);
  }
}
