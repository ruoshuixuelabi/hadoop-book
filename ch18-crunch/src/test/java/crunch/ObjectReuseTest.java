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
 * 对象复用测试
 *
 * 演示Crunch中的对象复用和值去重:
 * - 使用getDetachedValue()获取对象的独立副本
 * - 避免序列化过程中的对象共享问题
 *
 * 这在处理需要去重或需要独立修改对象的场景中很重要。
 */
public class ObjectReuseTest implements Serializable {

    /**
     * 字符串包装类
     *
     * 用于测试对象相等性和哈希
     */
    public static class StringWrapper {
        private String value;

        public StringWrapper() {
        }

        public StringWrapper(String value) {
            this.value = value;
        }

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

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /** 临时目录规则 */
    @Rule
    public transient TemporaryPath tmpDir = new TemporaryPath();

    /**
     * 测试去重功能
     *
     * 使用uniqueValues()获取每个键的唯一值集合
     */
    @Test
    public void test() throws IOException {
        String inputPath = tmpDir.copyResourceFileName("set2.txt");

        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> lines = pipeline.readTextFile(inputPath);

        // 将每行转换为(String, StringWrapper)对
        PTable<String, StringWrapper> table = lines.parallelDo(new DoFn<String, Pair<String,
            StringWrapper>>() {
            @Override
            public void process(String input, Emitter<Pair<String, StringWrapper>> emitter) {
                emitter.emit(Pair.of(input, new StringWrapper(input)));
            }
        }, Avros.tableOf(Avros.strings(), Avros.records(StringWrapper.class)));

        // 按键分组并去重
        PTable<String, Collection<StringWrapper>> uniques = uniqueValues(table);

        // 获取结果
        Map<String, Collection<StringWrapper>> materialisedUniques = uniques.materializeToMap();
        // 验证去重结果
        assertEquals(1, materialisedUniques.get("b").size());

        pipeline.run();
    }

    /**
     * 按键去重
     *
     * @param table 输入表
     * @return 去重后的表
     */
    public static <K, V> PTable<K, Collection<V>> uniqueValues(PTable<K, V> table) {
        PTypeFamily tf = table.getTypeFamily();
        final PType<V> valueType = table.getValueType();
        return table.groupByKey().mapValues("unique",
            new MapFn<Iterable<V>, Collection<V>>() {
                @Override
                public void initialize() {
                    // 初始化PType,使其可以正确处理值
                    valueType.initialize(getConfiguration());
                }

                @Override
                public Set<V> map(Iterable<V> values) {
                    Set<V> collected = new HashSet<V>();
                    for (V value : values) {
                        // 使用getDetachedValue获取独立副本