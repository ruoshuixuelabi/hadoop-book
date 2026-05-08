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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.longs;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PipelineExecution测试类.
 *
 * <p>本类测试Crunch管道的各种执行模式和特性,包括:
 * <ul>
 *   <li>异步执行与等待机制</li>
 *   <li>执行计划的DOT图形化导出</li>
 *   <li>管道重复运行的优化行为</li>
 *   <li>管道的取消与终止</li>
 *   <li>新目标(New Target)的增量执行</li>
 *   <li>ParallelDo融合与Sibling融合优化</li>
 * </ul>
 *
 * <p>测试采用TemporaryPath规则管理临时文件,确保测试环境的隔离性.
 *
 * // cc 管道执行测试 - 深入理解Crunch的执行模型
 * // vv PipelineExecution与PipelineResult - 执行状态与结果查询
 *
 * @see org.apache.crunch.Pipeline
 * @see org.apache.crunch.PipelineExecution
 */
public class PipelineExecutionTest implements Serializable {

  // 使用TemporaryPath规则提供临时目录,测试结束后自动清理
  public transient TemporaryPath tmpDir;

  /**
   * 测试异步执行模式.
   *
   * <p>演示如何使用runAsync()启动管道执行而不阻塞主线程,
   * 然后通过execution.get()等待执行完成.
   *
   * <p>测试流程:
   * <ol>
   *   <li>创建管道并读取输入文件</li>
   *   <li>使用MapFn将每行数据作为键创建PTable</li>
   *   <li>调用runAsync()异步启动执行</li>
   *   <li>可以在此期间执行其他操作</li>
   *   <li>调用get()阻塞等待结果</li>
   * </ol>
   *
   * @throws Exception 如果执行过程中发生错误
   */
  @Test
  public void testAsyncRunFuture() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");

    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    // by方法创建PTable,将每行数据作为键,值为空
    PTable<String,String> table = lines.by(new MapFn<String, String>() {
      @Override
      public String map(String s) {
        return s;
      }
    }, strings());
    pipeline.writeTextFile(table, tmpDir.getFileName("out"));

    // 异步启动管道执行,立即返回
    PipelineExecution execution = pipeline.runAsync();
    // meanwhile, do other things here
    // 调用get()会阻塞直到执行完成
    PipelineResult result = execution.get(); // blocks

    assertTrue(result.succeeded());
    pipeline.done();
  }

  /**
   * 测试执行计划的DOT文件生成(异步模式).
   *
   * <p>演示如何获取管道执行计划的DOT图形表示,并将其写入文件.
   * DOT格式可用于可视化管道拓扑结构.
   *
   * <p>管道流程:
   * input -> lines -> lower(ToLowerFn) -> counts -> inverseCounts(InversePairFn) -> hist(CountValuesFn)
   *
   * @throws Exception 如果执行过程中发生错误
   */
  @Test
  public void testInspectPlan() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    // parallelDo使用命名转换,便于在DOT图中识别
    PCollection<String> lower = lines.parallelDo("lower", new ToLowerFn(), strings());
    PTable<String, Long> counts = lower.count();
    PTable<Long, String> inverseCounts = counts.parallelDo("inverse",
        new InversePairFn<String, Long>(), tableOf(longs(), strings()));
    // mapValues命名转换并使用自定义函数处理值
    PTable<Long, Integer> hist = inverseCounts
        .groupByKey()
        .mapValues("count values", new CountValuesFn<String>(), ints());
    hist.write(To.textFile(outputPath), Target.WriteMode.OVERWRITE);

    PipelineExecution execution = pipeline.runAsync();
    // 获取DOT格式的执行计划
    String dot = execution.getPlanDotFile();
    Files.write(dot, dotFile(), Charsets.UTF_8);
    execution.waitUntilDone();
    pipeline.done();
  }

  /**
   * 测试执行计划的DOT文件生成(同步模式).
   *
   * <p>与testInspectPlan不同,此测试使用同步执行模式(done()),
   * 并通过管道配置获取DOT文件路径.
   *
   * @throws Exception 如果执行过程中发生错误
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
    // 同步执行管道
    PipelineResult result = pipeline.done();
    // 从管道配置中获取DOT文件路径
    String dot = pipeline.getConfiguration().get("crunch.planner.dotfile");
    Files.write(dot, dotFile(), Charsets.UTF_8);

    assertTrue(result.succeeded());
  }

  /**
   * 测试重复调用run()只会执行一次管道.
   *
   * <p>当管道没有任何新的输出目标时,重复运行不会触发新的MapReduce作业.
   * 这是Crunch的优化特性,避免无谓的计算.
   *
   * @throws IOException 如果发生IO错误
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
    pipeline.run();
    System.out.println("About to call run() for second time");
    // 第二次运行时没有新的输出,因此不会启动新的MR作业
    // the MR job is not run for a second time since there are no new outputs
    PipelineResult result = pipeline.run();
    // 由于没有实际执行作业,stageResults为空
    assertEquals(0, result.getStageResults().size());
    System.out.println("About to call done()");
    pipeline.done();
  }

  /**
   * 测试迭代算法场景(已忽略).
   *
   * <p>此测试演示迭代式管道的实现模式:循环检查PObject的值,
   * 当满足条件时继续执行下一轮计算.
   *
   * <p>注意:此测试被@Disabled标记,可能因为迭代时间不确定或资源消耗过大.
   *
   * @throws IOException 如果发生IO错误
   */
  @Test
  @Disabled
  public void testIterativeAlgorithm() throws IOException {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    int targetLength = 1;
    // 获取集合长度的PObject,可用于条件判断
    PObject<Long> length = lines.length();
    while (length.getValue() > targetLength) {
      System.out.println("Length: " + length.getValue());
      System.out.println("filter");
      // 使用FilterFn随机过滤一半的数据
      lines = lines.filter(new FilterFn<String>() {
        @Override
        public boolean accept(String input) {
          return Math.random() < 0.5;
        }
      });
      // 更新长度以继续迭代
      length = lines.length();
    }
    System.out.println("Final length: " + length.getValue());
    pipeline.done();
  }

  /**
   * 测试管道中断机制.
   *
   * <p>演示如何在另一个线程中运行管道,然后中断该线程.
   * 注意:中断不会真正取消已运行的MapReduce作业.
   *
   * @throws Exception 如果执行过程中发生错误
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

    // 在独立线程中启动管道执行
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        PipelineResult result = pipeline.run();
        // 中断发生时succeeded()可能返回false
        assertFalse(result.succeeded());
      }
    });
    thread.start();
    Thread.sleep(100);
    // 中断执行线程
    thread.interrupt();
    thread.join();
    // note that MR jobs are *not* cancelled
  }

  /**
   * 测试使用kill()终止管道执行.
   *
   * <p>通过PipelineExecution的kill()方法可以主动终止正在执行的管道.
   *
   * @throws Exception 如果执行过程中发生错误
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

    // 异步启动执行
    PipelineExecution execution = pipeline.runAsync();
    // 主动终止执行
    execution.kill();
    execution.waitUntilDone();
    // 验证管道状态为KILLED
    assertEquals(PipelineExecution.Status.KILLED, execution.getStatus());
  }

  /**
   * 测试使用cancel()取消管道执行.
   *
   * <p>cancel()方法可以中断正在运行的管道,并可选择是否等待中断完成.
   *
   * @param mayInterruptIfRunning 如果为true,则interrupt()被调用
   * @throws Exception 如果执行过程中发生错误
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
    // 取消执行,mayInterruptIfRunning=true
    execution.cancel(true);
    execution.waitUntilDone();
    assertEquals(PipelineExecution.Status.KILLED, execution.getStatus());
  }

  /**
   * 测试"New Target"优化:增量执行.
   *
   * <p>当管道有多个输出目标时,第一次运行会执行所有阶段,
   * 后续运行只会执行产生新输出的阶段,跳过没有变化的阶段.
   *
   * @throws Exception 如果执行过程中发生错误
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
    // 第一个输出目标
    pipeline.writeTextFile(hist, tmpDir.getFileName("hist1"));

    // 第一次完整执行,包含所有阶段
    PipelineResult run1 = pipeline.run();
    assertEquals(2, run1.getStageResults().size());

    // 第二个输出目标(新目标)
    pipeline.writeTextFile(hist, tmpDir.getFileName("hist2"));

    // 第二次运行只执行必要阶段,其他阶段被复用
    PipelineResult run2 = pipeline.run();
    // 只有产生hist2输出的阶段被执行
    assertEquals(1, run2.getStageResults().size());

    pipeline.done();
  }

  /**
   * 测试复杂管道的执行.
   *
   * <p>创建一个包含多次groupByKey和parallelDo转换的复杂管道,
   * 验证执行计划的正确性和阶段数量.
   *
   * <p>管道流程:
   * lines -> by -> groupByKey -> parallelDo(iso) -> by -> groupByKey -> parallelDo(iso2)
   *
   * @throws Exception 如果执行过程中发生错误
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
    // 第一次groupByKey和parallelDo转换
    PCollection<String> iso = table.groupByKey().parallelDo("iso",
        new DoFn<Pair<String, Iterable<String>>, String>() {
          @Override
          public void process(Pair<String, Iterable<String>> input,
              Emitter<String> emitter) {
            // 遍历所有值并发射
            for (String s : input.second()) {
              emitter.emit(s);
            }
          }
        }, strings());
    // 第二次by转换创建新的PTable
    PTable<String, String> table2 = iso.by(new MapFn<String, String>() {
      @Override
      public String map(String s) {
        return s;
      }
    }, strings());
    // 第二次groupByKey和parallelDo转换
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
    // 复杂管道执行后有两个阶段结果
    assertEquals(2, execution.get().getStageResults().size());

    pipeline.done();
  }

  /**
   * 测试ParallelDo Fusion优化.
   *
   * <p>当两个连续的parallelDo操作可以合并为一个时,
   * Crunch会自动进行融合优化,减少shuffle次数.
   *
   * <p>此测试创建两个连续的IdentityFn转换,它们应该被融合为一个阶段.
   *
   * @throws Exception 如果执行过程中发生错误
   */
  @Test
  public void testParallelDoFusion() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");

    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    // 两个连续的IdentityFn转换
    PCollection<String> t1 = lines.parallelDo("t1", IdentityFn.<String>getInstance(),
        strings());
    PCollection<String> t2 = t1.parallelDo("t2", IdentityFn.<String>getInstance(),
        strings());
    pipeline.writeTextFile(t2, tmpDir.getFileName("out"));

    PipelineExecution execution = pipeline.runAsync();
    execution.waitUntilDone();
    FileUtils.write(dotFile(), execution.getPlanDotFile(),
        "UTF-8");
    // 由于ParallelDo Fusion,两个操作被合并为一个阶段
    assertEquals(1, execution.get().getStageResults().size());

    pipeline.done();
  }

  /**
   * 测试Sibling Fusion优化.
   *
   * <p>当多个并行的PCollection共享相同的输入源时,
   * Crunch可以融合这些操作,减少重复计算.
   *
   * @throws Exception 如果执行过程中发生错误
   */
  @Test
  public void testSiblingFusion() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("set1.txt");

    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    // 两个从同一输入衍生的并行PCollection
    PCollection<String> t1 = lines.parallelDo("t1", IdentityFn.<String>getInstance(),
        strings());
    PCollection<String> t2 = lines.parallelDo("t2", IdentityFn.<String>getInstance(),
        strings());
    pipeline.writeTextFile(t1, tmpDir.getFileName("out1"));
    pipeline.writeTextFile(t2, tmpDir.getFileName("out2"));

    PipelineExecution execution = pipeline.runAsync();
    execution.waitUntilDone();
    FileUtils.write(dotFile(), execution.getPlanDotFile(),
        "UTF-8");
    // Sibling Fusion将两个操作融合为一个阶段
    assertEquals(1, execution.get().getStageResults().size());

    pipeline.done();
  }

  /**
   * 生成DOT文件路径.
   *
   * <p>根据当前测试方法名称生成唯一的DOT文件名,
   * 用于保存执行计划的可视化图形.
   *
   * @return 目标目录下的DOT文件
   */
  private File dotFile() {
    return new File("target/test.dot");
  }

}
