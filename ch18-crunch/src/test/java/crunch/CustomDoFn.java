package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

/**
 * 演示「DoFn 里持有不可序列化对象」该怎么写的自定义 DoFn
 *
 * <p>Crunch 的 DoFn 会被序列化后分发到集群各节点上执行，
 * 所以它自身以及它的字段都必须可序列化。
 * 但有些对象（比如带缓冲的辅助类）并不适合被序列化，
 * 惯用的办法是把字段声明成 {@code transient}，
 * 再在 {@code initialize()} 里于任务所在节点本地重新创建。
 */
public class CustomDoFn<S, T> extends DoFn<S, T> {

  /**
   * 一个故意不可序列化的辅助类，用来验证 transient 字段确实不会被序列化
   */
  static class NonSerializableHelper { }

  // transient 字段在序列化时被直接跳过，
  // 所以它不会让 DoFn 的序列化失败；代价是反序列化后必须重新赋值
  transient NonSerializableHelper helper;

  /**
   * 在任务节点上初始化 DoFn
   *
   * <p>initialize() 是在数据开始处理之前、在任务所在 JVM 内调用的，
   * 正是重建那些「不可序列化成员」的合适时机。
   */
  @Override
  public void initialize() {
    // 在本地新建辅助对象，而不是指望它被序列化传过来
    helper = new NonSerializableHelper();
  }

  /**
   * 处理一条输入记录，并把结果发射出去
   *
   * @param input 待处理的输入记录
   * @param emitter 结果发射器
   */
  @SuppressWarnings("unchecked")
  @Override
  public void process(S input, Emitter<T> emitter) {
    // use helper here
    // 这里做的是无检查类型转换：泛型参数在运行时已被擦除，
    // 所以只能靠强转把输入当作输出交给下游
    emitter.emit((T) input);
  }
}
