package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

/**
 * 把字符串转成小写的 DoFn
 *
 * <p>Crunch 里最简单的一类函数：输入一条记录，输出同样类型的一条记录。
 * 写成独立的顶层类（而不是匿名内部类）才能被序列化后分发到集群。
 */
public class ToLowerFn extends DoFn<String, String> {
  /**
   * 把输入字符串转成小写并发射出去
   *
   * @param input 输入字符串
   * @param emitter 结果发射器
   */
  @Override
  public void process(String input, Emitter<String> emitter) {
    // 转换后类型没变，可以直接发射；
    // DoFn 允许发射 0 条、1 条或多条结果，这里是 1 条
    emitter.emit(input.toLowerCase());
  }
}
