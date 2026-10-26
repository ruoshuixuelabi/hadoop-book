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
import org.junit.Rule;
import org.junit.Test;

import static org.apache.crunch.types.writable.Writables.strings;
import static org.junit.Assert.assertEquals;

/**
 * 反例测试：匿名内部类把外层对象一起带进了序列化
 *
 * <p>这里在实例方法里写了一个匿名 {@code DoFn}，
 * 匿名内部类会隐式持有外层实例的引用，
 * 而外层类本身并不是可序列化的，
 * 于是 DoFn 在被发往集群时序列化失败，
 * Crunch 把这个失败包装成 {@code CrunchRuntimeException} 抛出。
 * <p>这个测试的意义在于提醒：
 * 凡是会被 Crunch 分发出去的函数，都必须是可序列化的顶层类或静态嵌套类，
 * 匿名内部类只适合本地调试。
 */
public class NonSerializableOuterClass {

  /**
   * JUnit 规则：每个测试方法前后自动创建并清理一个临时目录
   */
  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  /**
   * 期望流水线在序列化 DoFn 时抛出 {@code CrunchRuntimeException}
   *
   * @throws IOException 读取输入文件失败
   */
  @Test(expected = CrunchRuntimeException.class)
  public void runPipeline() throws IOException {
    // 期望的结果：把 set1.txt 的四行转成小写后的内容
    List<String> expectedContent = Lists.newArrayList("b", "c", "a", "e");
    // 把测试资源复制到临时目录，得到一个本次运行独有的本地路径
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    // 用 MapReduce 后端执行，只有真正起作业才会触发 DoFn 的序列化
    Pipeline pipeline = new MRPipeline(getClass());
    // 读入文本文件，每一行就是 PCollection 里的一个元素
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    // 关键点：这里是实例方法中的匿名类，它会隐式捕获外层 this，
    // 而外层实例不可序列化，序列化失败就发生在这一步
    PCollection<String> lower = lines.parallelDo(new DoFn<String, String>() {
      /**
       * 把一行文本转成小写后发射出去
       *
       * @param input 输入的一行文本
       * @param emitter 结果发射器
       */
      @Override
      public void process(String input, Emitter<String> emitter) {
        emitter.emit(input.toLowerCase());
      }
    }, strings());
    // 正常情况下应该在这里断言内容；
    // 本例中上一步就已经抛异常，所以根本走不到这里
    assertEquals(expectedContent, Lists.newArrayList(lower.materialize()));
    // 结束流水线，阻塞等待作业跑完
    pipeline.done();
  }

}
