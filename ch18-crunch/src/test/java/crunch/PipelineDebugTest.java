package crunch;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.junit.Rule;
import org.junit.Test;

/**
 * 管道调试测试
 *
 * 演示如何启用Crunch的调试功能:
 * - enableDebug(): 启用调试输出
 * - crunch.log.job.progress: 记录作业进度
 */
public class PipelineDebugTest {

    /** 临时目录规则 */
    @Rule
    public transient TemporaryPath tmpDir = new TemporaryPath();

    /**
     * 测试调试功能
     *
     * 启用调试并配置作业进度日志
     */
    @Test
    public void testDebug() throws Exception {
        String inputPath = tmpDir.copyResourceFileName("set1.txt");
        Pipeline pipeline = new MRPipeline(getClass());
        // 启用调试模式
        pipeline.enableDebug();
        // 配置作业进度日志
        pipeline.getConfiguration().setBoolean("crunch.log.job.progress", true);
        PCollection<String> lines = pipeline.readTextFile(inputPath);
        pipeline.writeTextFile(lines, tmpDir.getFileName("out"));
        pipeline.done();
    }
}