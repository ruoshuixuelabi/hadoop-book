package crunch;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
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
 * 验证 Crunch 的「检查点（checkpoint）」机制：流水线重跑时可以跳过已算完的阶段
 *
 * <p>Crunch 的一次运行会被拆成若干个 stage（阶段）。如果把某个中间结果用
 * {@code Target.WriteMode.CHECKPOINT} 写出去，那么下次用同样的流水线再跑一遍时，
 * 只要检查点目录还在、写入成功（目录里有 {@code _SUCCESS} 标志文件），
 * 而且检查点比输入数据更新，这个 stage 就会被跳过，直接从检查点接着往下算。
 *
 * <p>本测试把同一条流水线跑两遍：第一遍要老老实实算完并写出检查点，所以有 2 个 stage；
 * 第二遍检查点被复用，前面的 stage 被跳过，只剩 1 个 stage。
 *
 * <p>测试类实现 {@code Serializable}，是为了在测试实例需要被序列化
 * （例如分发给集群节点）时不会因为外层引用而失败。
 */
public class CheckpointTest implements Serializable {

  /**
   * 临时目录规则：检查点目录和输出目录都放在这里，测试结束后自动清理
   *
   * <p>标成 {@code transient}，是因为本类实现了 {@code Serializable}，
   * 而 {@code TemporaryPath} 只在本机有效，序列化它没有意义。
   */
  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  /**
   * 测试名规则：提供当前测试方法的名字
   *
   * <p>通常用它给每个测试的输出目录起一个互不冲突的名字。
   */
  @Rule
  public transient TestName name = new TestName();

  /**
   * 跑两遍带检查点的流水线，比较两次运行的 stage 数量
   *
   * @throws Exception 复制资源文件或执行流水线失败时抛出
   */
  @Test
  public void testCheckpoint() throws Exception {

    // 复制输入文件到临时目录，它同时充当「源数据」
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    // 先睡两秒，让源文件的时间戳比稍后写出的检查点早一些。
    // 检查点只有在「比源数据更新」时才会被复用，而文件时间戳精度有限，
    // 留出这个间隔才能保证第二次运行一定命中检查点
    Thread.sleep(2000);

    // 第一遍：没有检查点可用，计算并写出检查点，一共 2 个 stage
    PipelineResult result1 = runCheckpointedPipeline(
        new MRPipeline(getClass()), inputPath);
    assertEquals(2, result1.getStageResults().size());

    // 第二遍：检查点被复用，前面的 stage 直接跳过，只剩 1 个 stage
    PipelineResult result2 = runCheckpointedPipeline(
        new MRPipeline(getClass()), inputPath);
    assertEquals(1, result2.getStageResults().size());

  }

  /**
   * 搭一条带检查点的流水线并把它跑完
   *
   * <p>流水线分两段：先统计每个单词出现多少次，并把「单词 -&gt; 次数」反写成
   * 「次数 -&gt; 单词」存进检查点目录；再从检查点读回来，按次数分组，
   * 统计「有多少个单词出现了这么多次」，得到一份次数直方图。
   *
   * <p>之所以要专门存检查点，是因为「统计次数并把键值反转」这一段是整条流水线里
   * 最花时间的一步，把中间结果存下来，下次运行就能直接从检查点接着往下算，
   * 不必把已经算过的部分重来一遍。
   *
   * @param pipeline 要执行的流水线
   * @param inputPath 输入文本所在的路径
   * @return 本次运行的执行结果，其中包含每个 stage 的信息
   * @throws Exception 执行流水线失败时抛出
   */
  private PipelineResult runCheckpointedPipeline(Pipeline pipeline,
      String inputPath) throws Exception {
    // 检查点目录和最终输出目录都放在临时目录下，互不干扰
    String checkpointPath = tmpDir.getFileName("checkpoint");
    String outputPath = tmpDir.getFileName("hist");
    // 读入文本，这一步仍然只是在搭 DAG
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    // count() 统计每个单词的出现次数，得到「单词 -> 次数」
    PTable<String, Long> counts = lines.count();
    // 把键值反转成「次数 -> 单词」，这样才能按次数做直方图
    PTable<Long, String> inverseCounts = counts.parallelDo(
        new InversePairFn<String, Long>(), tableOf(longs(), strings()));
    // 用 CHECKPOINT 模式写出：目录已存在且比源数据更新时，下次运行会直接复用它
    inverseCounts.write(To.sequenceFile(checkpointPath), Target.WriteMode.CHECKPOINT);
    // 从检查点接着算：按次数分组，再数出每组里有几个单词
    PTable<Long, Integer> hist = inverseCounts
        .groupByKey()
        .mapValues(new CountValuesFn<String>(), ints());
    // 直方图是最终结果，用 OVERWRITE 模式，每次重跑都直接覆盖
    hist.write(To.textFile(outputPath), Target.WriteMode.OVERWRITE);
    // done() 真正触发执行，并返回带有 stage 信息的运行结果
    return pipeline.done();
  }

}
