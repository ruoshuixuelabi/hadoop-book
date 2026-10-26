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
import org.junit.Rule;
import org.junit.Test;

import static org.apache.crunch.types.writable.Writables.strings;
import static org.junit.Assert.assertEquals;

/**
 * 验证 Crunch 的「物化（materialize）」语义：流水线究竟在什么时候跑起来
 *
 * <p>Crunch 的流水线是惰性的：{@code readTextFile}、{@code parallelDo} 只是往
 * 有向无环图（DAG）上挂节点，并不会立刻提交 MapReduce 作业。真正把数据算出来、
 * 拉回调用方进程的，是「物化」这个动作。
 *
 * <p>本测试用两条路径观察这一点：一条是直接调用 {@code PCollection.materialize()}，
 * 另一条是把集合包装成 {@code PObject} 后调用 {@code getValue()}。
 * 两者都会在「第一次取数据」的那一刻隐式触发整条流水线。
 */
public class MaterializeTest {

  /**
   * 临时目录规则：测试方法执行前自动建好临时目录，执行后自动删除
   *
   * <p>标成 {@code transient}，是为了在测试实例被序列化（例如随作业分发到集群节点）时，
   * 跳过这个只在本机有效的对象。
   */
  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  /**
   * 直接调用 {@code materialize()}：取数据的那一刻才真正跑作业
   *
   * <p>{@code materialize()} 返回的仍是一个惰性的 {@code Iterable}，
   * 真正的 {@code pipeline.run()} 发生在第一次迭代它的时候，
   * 所以下面那个 for 循环一开动，MapReduce 作业就被提交了。
   *
   * @throws IOException 复制测试资源文件失败时抛出
   */
  @Test
  public void testMaterializeCallsRunImplicitly() throws IOException {
    // 期望值按输入文件 set1.txt 的原始行序排列：b、c、a、e
    List<String> expectedContent = Lists.newArrayList("b", "c", "a", "e");
    // 把 classpath 里的资源复制到临时目录，避免多轮运行互相影响
    String inputPath = tmpDir.copyResourceFileName("set1.txt");

    // MRPipeline 是基于 Hadoop MapReduce 的流水线实现
    Pipeline pipeline = new MRPipeline(getClass());
    // 下面两步都只是在搭 DAG，此刻并没有任何作业被提交
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PCollection<String> lower = lines.parallelDo(new ToLowerFn(), strings());

    // materialize() 返回的仍是惰性对象，真正的计算推迟到第一次迭代
    Iterable<String> materialized = lower.materialize();
    // 第一次迭代触发 pipeline.run()，作业就在这里跑起来
    for (String s : materialized) { // pipeline is run
      System.out.println(s);
    }
    // 结果已被 MaterializableIterable 读进内存，再迭代一次不会重复提交作业
    assertEquals(expectedContent, Lists.newArrayList(materialized));

    // 作业刚才已经跑完了，done() 时没有待写的输出目标，
    // 于是直接返回一个不含任何 stage 的 PipelineResult
    PipelineResult result = pipeline.done();
    assertEquals(0, result.getStageResults().size());
  }

  /**
   * 通过 {@code PObject} 取值：同样会隐式触发流水线
   *
   * <p>{@code asCollection()} 得到的 {@code PObject} 内部持有的也是那个惰性
   * {@code Iterable}，第一次 {@code getValue()} 才会真正跑作业；
   * 算出来的值随后被缓存起来，再调 {@code getValue()} 不会重复计算。
   *
   * @throws IOException 复制测试资源文件失败时抛出
   */
  @Test
  public void testPObjectCallsRunImplicitly() throws IOException {
    // 期望值按输入文件 set1.txt 的原始行序排列：b、c、a、e
    List<String> expectedContent = Lists.newArrayList("b", "c", "a", "e");
    // 把 classpath 里的资源复制到临时目录，保证每轮运行的输入相互独立
    String inputPath = tmpDir.copyResourceFileName("set1.txt");

    // MRPipeline 是基于 Hadoop MapReduce 的流水线实现
    Pipeline pipeline = new MRPipeline(getClass());
    // readTextFile 与 parallelDo 都只是在搭 DAG，不会立刻启动作业
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PCollection<String> lower = lines.parallelDo(new ToLowerFn(), strings());

    // asCollection() 把整个集合包成一个「值只有一个」的 PObject
    PObject<Collection<String>> po = lower.asCollection();
    // 第一次 getValue() 触发 pipeline.run()，作业就在这里跑起来
    for (String s : po.getValue()) { // pipeline is run
      System.out.println(s);
    }
    // 第二次 getValue() 会命中 PObject 内部的缓存，不会再跑一遍作业
    assertEquals(expectedContent, po.getValue());

    // 打印一行日志，方便在作业日志里确认当前走到哪一步
    System.out.println("About to call done()");
    // 作业已经被上面那次 getValue() 跑完了，这里只剩收尾清理，
    // 所以返回的 PipelineResult 里一个 stage 也没有
    PipelineResult result = pipeline.done();
    assertEquals(0, result.getStageResults().size());
  }

}
