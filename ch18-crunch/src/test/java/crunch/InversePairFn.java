package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

/**
 * 键值对反转函数
 *
 * 将(Pair&lt;S, T&gt;)转换为(Pair&lt;T, S&gt;)。
 * 这在需要交换键值对的场景中很有用。
 *
 * 使用场景:
 * - 交换键值对以便按键聚合
 * - 倒排索引
 *
 * @param <S> 原始键类型
 * @param <T> 原始值类型
 */
public class InversePairFn<S, T> extends DoFn<Pair<S, T>, Pair<T, S>> {

    /**
     * 处理输入对并交换键值
     *
     * @param input 原始键值对
     * @param emitter 发射器
     */
    @Override
    public void process(Pair<S, T> input, Emitter<Pair<T, S>> emitter) {
        // 交换.first()和.second()
        emitter.emit(Pair.of(input.second(), input.first()));
    }
}