package crunch;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FileUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.FilterFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Target;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.longs;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;
import static org.junit.Assert.assertEquals;

/**
 * 检查点测试
 *
 * 演示Crunch的检查点功能:
 * - 使用Target.WriteMode.CHECKPOINT将中间结果持久化
 * - 第二次运行时无需重新计算检查点之前的步骤
 *
 * 这对于长管道很有用,可以避免重复计算。
 */
public class CheckpointTest implements Serializable {

    /** 临时目录规则 */
    @Rule
    public transient TemporaryPath tmpDir = new TemporaryPath();

    /** 测试方法名称规则 */
    @Rule
    public transient TestName name = new TestName();

    /**
     * 测试检查点功能
     *
     * 第一次运行: 需要计算所有步骤 (2个stage)
     * 第二次运行: 检查点已存在,只需计算后续步骤 (1个stage)
     */
    @Test
    public void testCheckpoint() throws Exception {
        String inputPath = tmpDir.copyResourceFileName("set1.txt");
        // 等待2秒确保时间戳不同
        Thread.sleep(2000);

        // 第一次运行管道
        PipelineResult result1 = runCheckpointedPipeline(
            new MRPipeline(getClass()), inputPath);
        // 两次stage: 计数 + 逆序计数
        assertEquals(2, result1.getStageResults().size());

        // 第二次运行管道
        PipelineResult result2 = runCheckpointedPipeline(
            new MRPipeline(getClass()), inputPath);
        // 一次stage: 逆序计数(因为计数已检查点化)
        assertEquals(1, result2.getStageResults().size());
    }

    /**
     * 运行带检查点的管道
     *
     * 管道流程:
     * 1. 读取文本
     * 2. 计数
     * 3. 检查点化(写入SequenceFile)
     * 4. 逆序
     * 5. 统计每组值数量
     *
     * @param pipeline Pipeline实例
     * @param inputPath 输入路径
     * @return 执行结果
     */
    private PipelineResult runCheckpointedPipeline(Pipeline pipeline,
        String inputPath) throws Exception {
        String checkpointPath = tmpDir.getFileName("checkpoint");
        String outputPath = tmpDir.getFileName("hist");

        // 读取文本
        PCollection<String> lines = pipeline.readTextFile(inputPath);
        // 计数
        PTable<String, Long> counts = lines.count();
        // 逆序键值对
        PTable<Long, String> inverseCounts = counts.parallelDo(
            new InversePairFn<String, Long>(), tableOf(longs(), strings()));
        // 检查点化: 将中间结果写入SequenceFile
        inverseCounts.write(To.sequenceFile(checkpointPath), Target.WriteMode.CHECKPOINT);
        // 统计每组值数量
        PTable<Long, Integer> hist = inverseCounts
            .groupByKey()
            .mapValues(new CountValuesFn<String