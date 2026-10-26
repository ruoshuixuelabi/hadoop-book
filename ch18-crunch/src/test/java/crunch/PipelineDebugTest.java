package crunch;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.junit.Rule;
import org.junit.Test;

/**
 * 演示怎样打开 Crunch 的调试模式
 *
 * <p>流水线出问题时，最耗时间的就是「不知道数据跑到哪一步就不对了」，
 * {@code enableDebug()} 会把各阶段的执行细节和中间结果打印出来，
 * 是排查数据流问题最直接的手段。
 */
public class PipelineDebugTest {
  /**
   * JUnit 规则：为每个测试方法准备一个独立的临时目录
   */
  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  /**
   * 打开调试模式，跑一遍最简单的流水线
   *
   * @throws Exception 流水线执行失败
   */
  @Test
  public void testDebug() throws Exception {
    // 把测试资源复制到临时目录，得到本地输入路径
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    // 用 MapReduce 后端执行
    Pipeline pipeline = new MRPipeline(getClass());
    // 打开调试：各阶段的执行细节和中间结果都会打出来
    pipeline.enableDebug();
    // 再打开作业进度日志，方便观察 Map/Reduce 任务的状态
    pipeline.getConfiguration().setBoolean("crunch.log.job.progress", true);
    // 读入文本文件，每行一个元素
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    // 把结果原样写回临时目录，便于人工检查输出内容
    pipeline.writeTextFile(lines, tmpDir.getFileName("out"));
    // 等待流水线真正执行完毕
    pipeline.done();
  }
}
