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
 * 原始操作测试
 *
 * 演示Crunch的基本PCollection和PTable操作:
 * - union: 并集
 * - parallelDo/map: 转换
 * - filter: 过滤
 * - groupByKey: 分组
 * - combineValues: 聚合
 * - count: 计数
 * - top/bottom: 取最大/最小
 *
 * 使用内存管道MemPipeline进行快速测试。
 */
public class PrimitiveOperationsTest implements Serializable {

    /** 临时目录规则 */
    @Rule
    public transient TemporaryPath tmpDir = new TemporaryPath();

    /**
     * 测试PCollection并集
     */
    @Test
    public void testPCollectionUnion() throws Exception {
        PCollection<Integer> a = MemPipeline.collectionOf(1, 3);
        PCollection<Integer> b = MemPipeline.collectionOf(2);
        PCollection<Integer> c = a.union(b);
        assertEquals("{2,1,3}", dump(c));
    }

    /**
     * 测试parallelDo转换
     */
    @Test
    public void testPCollectionParallelDo() throws Exception {
        PCollection<String> a = MemPipeline.collectionOf("cherry", "apple", "banana");
        PCollection<Integer> b = a.parallelDo(new DoFn<String, Integer>() {
            @Override
            public void process(String input, Emitter<Integer> emitter) {
                emitter.emit(input.length());
            }
        }, ints());
        assertEquals("{6,5,6}", dump(b));
    }

    /**
     * 测试map转换
     */
    @Test
    public void testPCollectionParallelDoMap() throws Exception {
        PCollection<String> a = MemPipeline.collectionOf("cherry", "apple", "banana");
        PCollection<Integer> b = a.parallelDo(new MapFn<String, Integer>() {
            @Override
            public Integer map(String input) {
                return input.length();
            }
        }, ints());
        assertEquals("{6,5,6}", dump(b));
    }

    /**
     * 测试filter过滤
     */
    @Test
    public void testPCollectionFilter() throws Exception {
        PCollection<String> a = MemPipeline.collectionOf("cherry", "apple", "banana");
        PCollection<String> b = a.filter(new FilterFn<String>() {
            @Override
            public boolean accept(String input) {
                return input.length() % 2 == 0; // 偶数长度
            }
        });
        assertEquals("{cherry,banana}", dump(b));
    }

    /**
     * 测试提取键值对
     */
    @Test
    public void testPCollectionParallelDoExtractKey() throws Exception {
        PCollection<String> a = MemPipeline.collectionOf("cherry", "apple", "banana");
        PTable<Integer, String> b = a.parallelDo(
            new DoFn<String, Pair<Integer, String>>() {
                @Override
                public void process(String input, Emitter<Pair<Integer, String>> emitter) {
                    emitter.emit(Pair.of(input.length(), input));
                }
            }, tableOf(ints(), strings()));
        assertEquals("{(6,cherry),(5,apple),(6,banana)}", dump(b));
    }

    /**
     * 测试分组和聚合
     */
    @Test
    public void testGrouping() throws Exception {
        String inputPath = tmpDir.copyResourceFileName("fruit.txt");
        Pipeline pipeline = new MRPipeline(getClass());

        PCollection<String> a = pipeline.readTextFile(inputPath);
        assertEquals("{cherry,apple,banana}", dump(a));

        // 按长度分组
        PTable<Integer, String> b = a.by(new MapFn<String, Integer>() {
            @Override
            public Integer map(String input) {
                return input.length();
            }
        }, ints());
        assertEquals("{(6,cherry),(5,apple),(6,banana)}", dump(b));

        PGroupedTable<Integer, String> c = b.groupByKey();
        assertEquals("{(5,[apple]),(6,[banana,cherry])}", dump(c));

        // 指定分区数
        c = b.groupByKey(2);
        String dumpc = dump(c);
        assertTrue(dumpc, "{(5,[apple]),(6,[banana,cherry])}".equals(dumpc) ||
            "{(6,[banana,cherry]),(5,[apple])}".equals(dumpc));

        c = b.groupByKey(); // 值迭代器只能使用一次

        // 自定义聚合函数
        PTable<Integer, String> d = c.combineValues(new CombineFn<Integer, String>() {
            @Override
            public void process(Pair<Integer, Iterable<String>> input,
                Emitter<Pair<Integer, String>> emitter) {
                StringBuilder sb = new StringBuilder();
                for (Iterator i = input.second().iterator(); i.hasNext(); ) {
                    sb.append(i.next());
                    if (i.hasNext()) { sb.append(";"); }
                }
                emitter.emit(Pair.of(input.first(), sb.toString()));
            }
        });
        assertEquals("{(5,apple),(6,banana;cherry)}", dump(d));

        c = b.groupByKey(); // 值迭代器只能使用一次

        // 使用内置聚合器
        PTable<Integer, String> e = c.combineValues(Aggregators.STRING_CONCAT(";",
            false));
        assertEquals("{(5,apple),(6,banana;cherry)}", dump(e));

        c = b.groupByKey();

        // 统计每组数量
        PTable<Integer, Integer> f = c.mapValues(new MapFn<Iterable<String>, Integer>() {
            @Override
            public Integer map(Iterable<String> input) {
                return Iterables.size(input);
            }
        }, ints());
        assertEquals("{(5,1),(6,2)}", dump(f));

        // 取消分组
        PTable<Integer, String> g = c.ungroup();
        assertEquals("{(5,apple),(6,banana),(6,cherry)}", dump(g));

    }

    /**
     * 测试collectValues
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

        // 收集同键的所有值
        PTable<Integer, Collection<String>> c = b.collectValues();
        assertEquals("{(5,[apple]),(6,[cherry, banana])}", dump(c));
    }

    /**
     * 测试PCollection统计操作
     */
    @Test
    public void testPCollectionStats() throws Exception {
        PCollection<String> a = MemPipeline.typedCollectionOf(strings(),
            "cherry", "apple", "banana", "banana");

        // 统计数量
        assertEquals((Long) 4L, a.length().getValue());
        // 最小值
        assertEquals("apple", a.min().getValue());
        // 最大值
        assertEquals("cherry", a.max().getValue());

        // 计数
        PTable<String, Long> b = a.count();
        assertEquals("{(apple,1),(banana,2),(cherry,1)}", dump(b));

        // 取Top N
        PTable<String, Long> c = b.top(1);
        assertEquals("{(banana,2)}", dump(c));

        // 取Bottom N
        PTable<String, Long> d = b.bottom(2);
        assertEquals("{(apple,1),(cherry,1)}", dump(d));
    }

    /**
     * 测试PTable操作
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

        // 提取键
        PCollection<Integer> c = b.keys();
        assertEquals("{6,5,6}", dump(c));

        // 提取值
        PCollection<String> d = b.values();
        assertEquals("{cherry,apple,banana}", dump(d));

        // 转换键
        PTable<Double, String> e = b.mapKeys(new MapFn<Integer, Double>() {
            @Override
            public Double map(Integer input) {
                return input.doubleValue();
            }
        }, doubles());
        assertEquals("{(6.0,cherry),(5.0,apple),(6.0,banana)}", dump(e));

        // 转换值
        PTable<Integer, String> f = b.mapValues(new MapFn<String, String>() {
            @Override
            public String map(String input) {
                return input.substring(0, 1);
            }
        }, strings());
        assertEquals("{(6,c),(5,a),(6,b)}", dump(f));
    }
}