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
 * PCollection工具类
 *
 * 提供PCollection、PTable和PGroupedTable的调试输出方法。
 * 用于将分布式集合转换为字符串表示以便查看。
 */
public class PCollections {

    /**
     * 将PCollection转换为字符串
     *
     * 格式: {element1,element2,...}
     *
     * @param collection 输入集合
     * @return 字符串表示
     */
    public static <S> String dump(PCollection<S> collection) {
        StringBuilder sb = new StringBuilder("{");
        for (Iterator<S> i = collection.materialize().iterator(); i.hasNext(); ) {
            sb.append(i.next());
            if (i.hasNext()) {
                sb.append(",");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * 将PTable转换为字符串
     *
     * 格式: {(key1,value1),(key2,value2),...}
     *
     * @param table 输入表
     * @return 字符串表示
     */
    public static <K, V> String dump(PTable<K, V> table) {
        StringBuilder sb = new StringBuilder("{");
        for (Iterator<Pair<K, V>> i = table.materialize().iterator(); i.hasNext(); ) {
            Pair<K, V> pair = i.next();
            sb.append("(").append(pair.first()).append(",").append(pair.second()).append(")");
            if (i.hasNext()) {
                sb.append(",");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * 将PGroupedTable转换为字符串
     *
     * 格式: {(key1,[value1,value2]),(key2,[value3]),...}
     *
     * @param groupedTable 输入分组表
     * @return 字符串表示
     */
    public static <K, V> String dump(PGroupedTable<K, V> groupedTable) {
        return dump(groupedTable.mapValues(new MapFn<Iterable<V>, String>() {
            @Override
            public String map(Iterable<V> input) {
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                for (Iterator<V> i = input.iterator(); i.hasNext(); ) {
                    sb.append(i.next());
                    if (i.hasNext()) {
                        sb.append(",");
                    }
                }
                sb.append("]");
                return sb.toString();
            }
        }, strings()));
    }

    /**
     * 反转PTable的键值对
     *
     * @param table 输入表
     * @return 键值反转后的表
     */
    public static <K, V> PTable<V, K> invert(PTable<K, V> table) {
        PTypeFamily tf = table.getTypeFamily();
        PTableType<V, K> type = tf.tableOf(table.getValueType(), table.getKeyType());
        return table.parallelDo(new InversePairFn<K, V>(), type);
    }
}