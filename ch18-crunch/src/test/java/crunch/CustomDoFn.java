package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

/**
 * 自定义DoFn示例
 *
 * 演示如何在DoFn中使用非序列化辅助类:
 * - 在initialize()方法中初始化非序列化字段
 * - 使用transient修饰非序列化字段
 *
 * 这在需要使用无法序列化的库时很有用。
 *
 * @param <S> 输入类型
 * @param <T> 输出类型
 */
public class CustomDoFn<S, T> extends DoFn<S, T> {

    /**
     * 非序列化辅助类
     *
     * 由于DoFn需要序列化,这个内部类不会被序列化
     */
    static class NonSerializableHelper { }

    /** transient字段不会被序列化 */
    transient NonSerializableHelper helper;

    /**
     * 初始化方法
     *
     * 在每个任务开始时调用,用于初始化非序列化字段
     */
    @Override
    public void initialize() {
        helper = new NonSerializableHelper();
    }

    /**
     * 处理输入并输出
     *
     * @param input 输入元素
     * @param emitter 发射器
     */
    @SuppressWarnings("unchecked")
    @Override
    public void process(S input, Emitter<T> emitter) {
        // 使用helper
        emitter.emit((T) input);
    }
}