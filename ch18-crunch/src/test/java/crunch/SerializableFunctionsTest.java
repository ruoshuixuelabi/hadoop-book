package crunch;

import java.io.IOException;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.junit.jupiter.api.Test;

import static org.apache.crunch.types.avro.Avros.strings;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 序列化函数测试
 *
 * 演示Crunch如何在分布式环境中处理DoFn序列化:
 * - 测试自定义DoFn是否可以正确序列化
 * - 验证initialize()方法在反序列化后被调用
 */
public class SerializableFunctionsTest {

    /** 临时目录规则 */
    public transient TemporaryPath tmpDir;

    /**
     * 测试initialize方法被调用
     *
     * CustomDoFn的initialize()方法会创建NonSerializableHelper实例
     * 如果序列化/反序列化有问题,会抛出异常
     */
    @Test
    public void testInitialize() throws IOException {
        String inputPath = tmpDir.copyResourceFileName("set1.txt");
        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> lines = pipeline.readTextFile(inputPath);
        // 使用CustomDoFn,如果initialize()没被调用会失败
        long len = lines.parallelDo(new CustomDoFn<String, String>(), strings())
            .length().getValue();
        assertEquals(4, len);
        pipeline.done();
    }
}