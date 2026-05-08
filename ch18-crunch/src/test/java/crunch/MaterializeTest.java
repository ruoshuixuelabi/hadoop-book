package crunch;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.junit.jupiter.api.Test;

import static org.apache.crunch.types.writable.Writables.strings;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 物化测试
 *
 * 演示Crunch的物化功能:
 * - PCollection.materialize(): 将分布式集合物化为本地迭代器
 * - PObject: 单值对象,可以立即获取结果
 *
 * 这些操作会触发管道的实际执行。
 */
public class MaterializeTest {

    /** 临时目录规则 */
    public transient TemporaryPath tmpDir;

    /**
     * 测试materialize()触发管道执行
     *
     * materialize()会触发管道的惰性计算
     */
    @Test
    public void testMaterializeCallsRunImplicitly() throws IOException {
        List<String> expectedContent = Lists.newArrayList("b", "c", "a", "e");
        String inputPath = tmpDir.copyResourceFileName("set1.txt");

        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> lines = pipeline.readTextFile(inputPath);
        PCollection<String> lower = lines.parallelDo(new ToLowerFn(), strings());

        // 物化会触发管道执行
        Iterable<String> materialized = lower.materialize();
        for (String s : materialized) { // 管道运行
            System.out.println(s);
        }
        assertEquals(expectedContent, Lists.newArrayList(materialized));

        PipelineResult result = pipeline.done();
        // materialize触发运行,但没有新的stage
        assertEquals(0, result.getStageResults().size());
    }

    /**
     * 测试PObject触发管道执行
     *
     * PObject.getValue()会触发管道执行
     */
    @Test
    public void testPObjectCallsRunImplicitly() throws IOException {
        List<String> expectedContent = Lists.newArrayList("b", "c", "a", "e");
        String inputPath = tmpDir.copyResourceFileName("set1.txt");

        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> lines = pipeline.readTextFile(inputPath);
        PCollection<String> lower = lines.parallelDo(new ToLowerFn(), strings());

        // asCollection返回PObject
        PObject<Collection<String>> po = lower.asCollection();
        for (String s : po.getValue()) { // 管道运行
            System.out.println(s);
        }
        assertEquals(expectedContent, po.getValue());

        System.out.println("About to call done()");
        PipelineResult result = pipeline.done();
        assertEquals(0, result.getStageResults().size());
    }

}