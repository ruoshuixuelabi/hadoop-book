package crunch;

import java.io.IOException;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.crunch.types.avro.Avros.strings;
import static org.junit.Assert.assertEquals;

/**
 * 验证带 transient 字段的 DoFn 能被正常分发和执行
 *
 * <p>{@code CustomDoFn} 里刻意放了一个不可序列化的辅助对象，
 * 用 transient 标注它并在 initialize() 中重建，
 * 这个测试用一次真实的 MapReduce 流水线来确认
 * 「不可序列化的成员不会拖垮 DoFn 的序列化」。
 */
public class SerializableFunctionsTest {

  /**
   * JUnit 规则：每个测试方法前后自动创建并清理一个临时目录
   */
  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  /**
   * 用自定义 DoFn 处理输入，并统计处理结果的条数
   *
   * @throws IOException 读取输入文件失败
   */
  @Test
  public void testInitialize() throws IOException {
    // 把测试资源复制到临时目录，得到本地输入路径
    String inputPath = tmpDir.copyResourceFileName("set1.txt");
    // 用 MapReduce 后端跑，DoFn 才会被真正序列化后分发到各节点
    Pipeline pipeline = new MRPipeline(getClass());
    // 读入文本文件，每行一个元素
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    // 先过一遍自定义 DoFn，再用 length() 把结果聚合成一个数字
    long len = lines.parallelDo(new CustomDoFn<String, String>(), strings())
        .length().getValue();
    // 输入文件一共 4 行，转换之后仍然是 4 条记录
    assertEquals(4, len);
    // 结束流水线，阻塞等待作业跑完
    pipeline.done();
  }
}
