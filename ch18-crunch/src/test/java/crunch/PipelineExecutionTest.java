package crunch;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Crunch 管道执行（Pipeline Execution）的测试
 *
 * <p>前面几个测试类关注的是「数据长什么样」，本类关注的是「管道怎么跑起来」：
 * 同步执行与异步执行的区别、作业能不能重复运行、如何中途终止作业、
 * 以及在计划阶段如何把执行计划导出成 dot 文件来观察。
 *
 * <p>几个关键概念：
 *
 * <p>{@code pipeline.runAsync()} 立刻返回 {@code PipelineExecution}，
 * 作业在后台继续跑；{@code pipeline.done()} 是同步的，会阻塞到作业结束。
 *
 * <p>{@code PipelineExecution} 可以被 {@code kill()} 或 {@code cancel()} 终止。
 *
 * <p>{@code pipeline.run()} 只提交「新增输出」的阶段，
 * 已经跑过的阶段不会重复跑，这正是增量执行的基础。
 */
public class PipelineExecutionTest implements Serializable {

  /**
   * 测试期间使用的临时目录，测试结束后会被自动清理
   *
   * <p>{@code transient} 表示这个字段不参与序列化：它只是 JUnit 的运行环境对象，
   * 只在测试进程内有意义，不能随作业被序列化到 MapReduce 任务里去。
   */
  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  /**
   * 当前正在运行的测试方法名，用来给导出的 dot 文件命名
   *
   * <p>{@code TestName} 会在每个测试开始前自动更新成当前方法名，
   * 于是每个用例生成的 dot 文件都是独立的，不会互相覆盖。
   */
  @Rule
  public transient TestName name = new TestName();

  /**
   * 异步执行：先 {@code runAsync()} 拿到执行句柄，再决定何时等待结果
   *
   * <p>{@code runAsync()} 会立刻提交作业并返回一个 {@code PipelineExecution}，
   * 主线程可以继续做别的事情；调用 {@code execution.get()} 才会阻塞到作业结束，
   * 并返回最终的 {@code PipelineResult}。这正是「提交」与「等待」可以分开的意义。
   *
   * @throws Exception 复制测试资源或作业执行失败
   */
  @Test
  public void testAsyncRunFuture() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");

    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PTable<String,String> table = lines.by(new MapFn<String, String>() {
      @Override
      public String map(String s) {
        return s;
      }
    }, strings());
    pipeline.writeTextFile(table, tmpDir.getFileName("out"));

    // runAsync 只是提交作业，真正的作业在后台跑
    PipelineExecution execution = pipeline.runAsync();
    // meanwhile, do other things here
    // get() 会一直阻塞到作业跑完，此时才拿到 PipelineResult
    PipelineResult result = execution.get(); // blocks

    // succeeded() 用来确认作业是正常跑完的，而不是失败退出
    assertTrue(result.succeeded());
    pipeline.done();
  }

  /**
   * 在异步执行时导出执行计划（dot 文件）
   *
   * <p>{@code getPlanDotFile()} 返回的是「计划文件在集群上的路径」，
   * 而不是文件内容，所以还要把它下载到本地再写成 dot 文件。
   * 用 Graphviz 打开这个文件，就能直观看到 Crunch 把这条管道切成了哪几个阶段。
   *
   * @throws Exception 复制测试资源、下载计划文件或作业执行失败
   */
  @Test
  public void testInspectPlan() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PCollection<String> lower = lines.parallelDo("lower", new ToLowerFn(), strings());
    PTable<String, Long> counts = lower.count();
    PTable<Long, String> inverseCounts = counts.parallelDo("inverse",
        new InversePairFn<String, Long>(), tableOf(longs(), strings()));
    PTable<Long, Integer> hist = inverseCounts
        .groupByKey()
        .mapValues("count values", new CountValuesFn<String>(), ints());
    // 先写出一个输出目标，管道里有了输出才会真正生成阶段
    hist.write(To.textFile(outputPath), Target.WriteMode.OVERWRITE);
    PipelineExecution execution = pipeline.runAsync();
    // getPlanDotFile 给出的是 HDFS 上的路径，Files.write 会把它下载到本地
    String dot = execution.getPlanDotFile();
    Files.write(dot, dotFile(), Charsets.UTF_8);
    // 导完计划还要等作业真正结束，否则测试可能在作业还在跑时就结束
    execution.waitUntilDone();
    pipeline.done();
  }

  /**
   * 在同步执行时导出执行计划
   *
   * <p>同步方式下拿不到 {@code PipelineExecution}，
   * 于是改从管道配置里读 {@code crunch.planner.dotfile} 这个属性，
   * 它记录了计划文件被写到哪里。其余流程与异步版本一致。
   *
   * @throws Exception 复制测试资源、下载计划文件或作业执行失败
   */
  @Test
  public void testInspectPlanSynchronous() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PCollection<String> lower = lines.parallelDo("lower", new ToLowerFn(), strings());
    PTable<String, Long> counts = lower.count();
    PTable<Long, String> inverseCounts = counts.parallelDo("inverse",
        new InversePairFn<String, Long>(), tableOf(longs(), strings()));
    PTable<Long, Integer> hist = inverseCounts
        .groupByKey()
        .mapValues("count values", new CountValuesFn<String>(), ints());
    hist.write(To.textFile(outputPath));
    // done() 是同步的：提交作业并一直等到它跑完
    PipelineResult result = pipeline.done();
    // 同步模式下计划文件路径记在管道配置里
    String dot = pipeline.getConfiguration().get("crunch.planner.dotfile");
    Files.write(dot, dotFile(), Charsets.UTF_8);

    assertTrue(result.succeeded());
  }

  /**
   * 连续调用两次 {@code run()}，第二次不会重复提交作业
   *
   * <p>Crunch 会记住哪些输出已经算过了；第二次 {@code run()} 时管道里没有新增输出，
   * 于是没有任何阶段需要执行，{@code getStageResults()} 返回空的列表。
   * 这个「按输出增量执行」的机制是长链式管道能反复迭代的基础。
   *
   * @throws IOException 复制测试资源失败
   */
  @Test
  public void testCallingRunTwiceOnlyRunsOneJob() throws IOException {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");

    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PTable<String,String> table = lines.by(new MapFn<String, String>() {
      @Override
      public String map(String s) {
        return s;
      }
    }, strings());
    pipeline.writeTextFile(table, tmpDir.getFileName("out"));

    System.out.println("About to call run() for first time");
    // 第一次 run 会真正提交作业
    pipeline.run();
    System.out.println("About to call run() for second time");
    // the MR job is not run for a second time since there are no new outputs
    // 第二次 run 没有任何新增输出，所以阶段结果数为 0
    PipelineResult result = pipeline.run();
    assertEquals(0, result.getStageResults().size());
    System.out.println("About to call done()");
    pipeline.done();
  }

  /**
   * 迭代式算法：反复过滤直到集合足够小
   *
   * <p>本用例演示「迭代」在 Crunch 里怎么做：每轮随机丢掉一半元素，
   * 重新统计长度，直到长度降到目标值为止。
   *
   * <p>它目前被 {@code @Ignore} 掉，原因是这个循环每次都要真的提交一个作业来求长度，
   * 而循环条件依赖上一轮的结果，跑起来非常慢，不适合放在常规测试里。
   *
   * @throws IOException 复制测试资源或作业执行失败
   */
  @Test
  @Ignore
  public void testIterativeAlgorithm() throws IOException {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    int targetLength = 1;
    // length() 返回 PObject，getValue() 会触发一次真实计算拿到当前长度
    PObject<Long> length = lines.length();
    while (length.getValue() > targetLength) {
      System.out.println("Length: " + length.getValue());
      System.out.println("filter");
      // 每轮随机保留一半元素，把结果重新赋值给 lines
      lines = lines.filter(new FilterFn<String>() {
        @Override
        public boolean accept(String input) {
          return Math.random() < 0.5;
        }
      });
      length = lines.length();
    }
    System.out.println("Final length: " + length.getValue());
    pipeline.done();
  }

  /**
   * 中断正在运行的线程，观察作业是否受影响
   *
   * <p>这里在另一个线程里同步执行管道，然后把这个线程 {@code interrupt()} 掉。
   * 断言作业因此以失败告终——说明 Crunch 会把线程的中断状态传递给作业执行过程。
   *
   * <p>但要注意：中断只影响客户端线程，已经提交到集群上的 MapReduce 作业并不会被取消，
   * 它们仍会继续跑完，这属于框架的已知限制。
   *
   * @throws Exception 复制测试资源或作业执行失败
   */
  @Test
  public void testInterruptPipeline() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    String outputPath = tmpDir.getFileName("out");
    final Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PTable<String, Long> counts = lines.count();
    PTable<Long, String> inverseCounts = counts.parallelDo(
        new InversePairFn<String, Long>(), tableOf(longs(), strings()));
    PTable<Long, Integer> hist = inverseCounts
        .groupByKey()
        .mapValues(new CountValuesFn<String>(), ints());
    hist.write(To.textFile(outputPath), Target.WriteMode.OVERWRITE);

    // 在独立线程里跑作业，主线程才有机会去中断它
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        PipelineResult result = pipeline.run();
        assertFalse(result.succeeded());
      }
    });
    thread.start();
    // 稍等一小段时间，确保作业已经提交、正在运行
    Thread.sleep(100);
    thread.interrupt();
    thread.join();
    // note that MR jobs are *not* cancelled
  }

  /**
   * 用 {@code kill()} 强制终止作业
   *
   * <p>{@code kill()} 是「立即处决」：作业不再有机会走到正常结束，
   * 执行状态会变成 {@code KILLED}。
   *
   * @throws Exception 复制测试资源或作业执行失败
   */
  @Test
  public void testKillPipeline() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PTable<String, Long> counts = lines.count();
    PTable<Long, String> inverseCounts = counts.parallelDo(
        new InversePairFn<String, Long>(), tableOf(longs(), strings()));
    PTable<Long, Integer> hist = inverseCounts
        .groupByKey()
        .mapValues(new CountValuesFn<String>(), ints());
    hist.write(To.textFile(outputPath), Target.WriteMode.OVERWRITE);

    PipelineExecution execution = pipeline.runAsync();
    // kill 之后再等作业收尾，此时状态应已被标记为 KILLED
    execution.kill();
    execution.waitUntilDone();
    assertEquals(PipelineExecution.Status.KILLED, execution.getStatus());
  }

  /**
   * 用 {@code cancel()} 取消作业
   *
   * <p>{@code cancel(true)} 表示「连同正在运行的阶段一起取消」。
   * 与 {@code kill()} 不同，取消走的是「尽量优雅地停止」的路径，
   * 但由于作业仍在集群上跑，最终状态同样表现为 {@code KILLED}。
   *
   * @throws Exception 复制测试资源或作业执行失败
   */
  @Test
  public void testCancelPipeline() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PTable<String, Long> counts = lines.count();
    PTable<Long, String> inverseCounts = counts.parallelDo(
        new InversePairFn<String, Long>(), tableOf(longs(), strings()));
    PTable<Long, Integer> hist = inverseCounts
        .groupByKey()
        .mapValues(new CountValuesFn<String>(), ints());
    hist.write(To.textFile(outputPath), Target.WriteMode.OVERWRITE);

    PipelineExecution execution = pipeline.runAsync();
    // cancel(true) 传 true 表示连正在执行的阶段也要取消
    execution.cancel(true);
    execution.waitUntilDone();
    assertEquals(PipelineExecution.Status.KILLED, execution.getStatus());
  }

  /**
   * 管道里新增输出目标后，再 {@code run()} 只跑新增的那部分
   *
   * <p>第一次 {@code run()} 时管道只有一个输出目标，需要两个阶段才能算完；
   * 之后又加了一个输出目标，第二次 {@code run()} 就只需再跑一个阶段。
   * 这说明 Crunch 是按「新增输出」来划分增量执行的粒度的。
   *
   * @throws Exception 复制测试资源或作业执行失败
   */
  @Test
  public void testNewTarget() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");

    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PTable<String, Long> counts = lines.count();
    PTable<Long, String> inverseCounts = counts.parallelDo(
        new InversePairFn<String, Long>(), tableOf(longs(), strings()));
    PTable<Long, Integer> hist = inverseCounts
        .groupByKey()
        .mapValues(new CountValuesFn<String>(), ints());
    // 先加第一个输出目标，此时整条管道都要算，所以是两个阶段
    pipeline.writeTextFile(hist, tmpDir.getFileName("hist1"));

    PipelineResult run1 = pipeline.run();
    assertEquals(2, run1.getStageResults().size());

    // 再加第二个输出目标，第二次只需多跑一个阶段
    pipeline.writeTextFile(hist, tmpDir.getFileName("hist2"));

    PipelineResult run2 = pipeline.run();
    assertEquals(1, run2.getStageResults().size());

    pipeline.done();
  }

  /**
   * 较复杂的管道：两次「分组再展开」的往返
   *
   * <p>管道先把元素变成表，按键分组后把每组的值又摊平成集合，
   * 然后再走一遍同样的过程。这类「分组—展开」的往返通常无法在同一个阶段里完成，
   * 因此 Crunch 会把它切成多个阶段，本用例断言正好是两个阶段。
   *
   * @throws Exception 复制测试资源或作业执行失败
   */
  @Test
  public void testComplexPipeline() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");

    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PTable<String, String> table = lines.by(new MapFn<String, String>() {
      @Override
      public String map(String s) {
        return s;
      }
    }, strings());
    // groupByKey 之后是分组表，parallelDo 的输入是「键 + 该键对应的所有值」
    PCollection<String> iso = table.groupByKey().parallelDo("iso",
        new DoFn<Pair<String, Iterable<String>>, String>() {
      @Override
      public void process(Pair<String, Iterable<String>> input,
          Emitter<String> emitter) {
        // 把这一组的值逐个 emit 出去，摊平成一个新的集合
        for (String s : input.second()) {
          emitter.emit(s);
        }
      }
    }, strings());
    PTable<String, String> table2 = iso.by(new MapFn<String, String>() {
      @Override
      public String map(String s) {
        return s;
      }
    }, strings());
    PCollection<String> iso2 = table2.groupByKey().parallelDo("iso",
        new DoFn<Pair<String, Iterable<String>>, String>() {
          @Override
          public void process(Pair<String, Iterable<String>> input,
              Emitter<String> emitter) {
            for (String s : input.second()) {
              emitter.emit(s);
            }
          }
        }, strings());
    pipeline.writeTextFile(iso2, tmpDir.getFileName("out"));

    PipelineExecution execution = pipeline.runAsync();
    execution.waitUntilDone();
    FileUtils.write(dotFile(), execution.getPlanDotFile(),
        "UTF-8");
    // 两次「分组再展开」让 Crunch 无法融成一个阶段，于是被切成两个阶段
    assertEquals(2, execution.get().getStageResults().size());

    pipeline.done();
  }

  /**
   * 串联的两个 {@code parallelDo} 会被融合成一个阶段
   *
   * <p>两个 {@code IdentityFn} 什么都没做，Crunch 的优化器会把它们合并执行，
   * 所以整条管道只需要一个阶段。阶段数是观察 Crunch 优化效果最直观的指标。
   *
   * @throws Exception 复制测试资源或作业执行失败
   */
  @Test
  public void testParallelDoFusion() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");

    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PCollection<String> t1 = lines.parallelDo("t1", IdentityFn.<String>getInstance(),
        strings());
    // t1 和 t2 都是恒等变换，理论上可以被优化器合并
    PCollection<String> t2 = t1.parallelDo("t2", IdentityFn.<String>getInstance(),
        strings());
    pipeline.writeTextFile(t2, tmpDir.getFileName("out"));

    PipelineExecution execution = pipeline.runAsync();
    execution.waitUntilDone();
    FileUtils.write(dotFile(), execution.getPlanDotFile(),
        "UTF-8");
    // 断言阶段数为 1，说明两个 parallelDo 被融合成了同一个阶段
    assertEquals(1, execution.get().getStageResults().size());

    pipeline.done();
  }

  /**
   * 从同一个源出发的两个 {@code parallelDo} 也会被融合
   *
   * <p>这里两个 {@code parallelDo} 都以 {@code lines} 为输入，属于「兄弟」关系。
   * 因为它们的输入相同、逻辑又都是恒等变换，Crunch 把它们放进了同一个阶段，
   * 于是依然只有一个阶段。两个输出目标不会因此多出阶段。
   *
   * @throws Exception 复制测试资源或作业执行失败
   */
  @Test
  public void testSiblingFusion() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");

    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PCollection<String> t1 = lines.parallelDo("t1", IdentityFn.<String>getInstance(),
        strings());
    // t1 与 t2 都直接来自 lines，是一对可以共享同一个阶段的分支
    PCollection<String> t2 = lines.parallelDo("t2", IdentityFn.<String>getInstance(),
        strings());
    pipeline.writeTextFile(t1, tmpDir.getFileName("out1"));
    pipeline.writeTextFile(t2, tmpDir.getFileName("out2"));

    PipelineExecution execution = pipeline.runAsync();
    execution.waitUntilDone();
    FileUtils.write(dotFile(), execution.getPlanDotFile(),
        "UTF-8");
    // 兄弟分支同样被融合，所以阶段数仍是 1
    assertEquals(1, execution.get().getStageResults().size());

    pipeline.done();
  }

  /**
   * 生成本次测试的 dot 文件路径
   *
   * <p>文件放在 {@code target} 目录下，文件名取当前测试方法名，
   * 这样每个用例导出的执行计划互不覆盖，排查问题时可以逐个对照。
   *
   * @return 指向本地 dot 文件的 {@link File} 对象
   */
  private File dotFile() {
    return new File("target/" + name.getMethodName() + ".dot");
  }

}
