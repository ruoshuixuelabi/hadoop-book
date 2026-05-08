package crunch;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.junit.jupiter.api.Test;

import static org.apache.crunch.types.writable.Writables.strings;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 非序列化外部类测试
 *
 * 演示匿名DoFn的问题:
 * - 如果匿名DoFn引用了外部类的非序列化字段,会抛出CrunchRuntimeException
 *
 * 这是一个负面测试,用于验证序列化的严格要求。
 */
public class NonSerializableOuterClass {

    /**
     * 临时目录规则
     */
    public transient TemporaryPath tmpDir = new TemporaryPath();

    /**
     * 测试非序列化外部类会失败
     *
     * 匿名DoFn引用了外部类,但外部类未实现Serializable
     * 应该抛出CrunchRuntimeException
     */
    @Test(expected = CrunchRuntimeException.class)
    public void runPipeline() throws IOException {
        List<String> expectedContent = Lists.newArrayList("b", "c", "a", "e");
        String inputPath = tmpDir.copyResourceFileName("set1.txt");
        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> lines = pipeline.readTextFile(inputPath);
        // 匿名DoFn引用了外部类NonSerializableOuterClass
        // 由于外部类未实现Serializable,这里会失败
        PCollection<String> lower = lines.parallelDo(new DoFn<String, String>() {
            @Override
            public void process(String input, Emitter<String> emitter) {
                emitter.emit(input.toLowerCase());
            }
        }, strings());
        assertEquals(expectedContent, Lists.newArrayList(lower.materialize()));
        pipeline.done();
    }

}