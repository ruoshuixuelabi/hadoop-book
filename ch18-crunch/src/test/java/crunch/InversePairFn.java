package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

/**
 * 把一个 {@code Pair} 的两个分量对调后输出
 *
 * <p>Crunch 的 {@code Pair} 是不可变的，
 * 所以「交换键和值」只能靠重新构造一个新的 {@code Pair} 来实现，
 * 无法就地修改已有的键值对。
 */
public class InversePairFn<S, T> extends DoFn<Pair<S, T>, Pair<T, S>> {
  /**
   * 交换输入键值对的两个分量，并把交换后的结果发射出去
   *
   * @param input 输入的键值对
   * @param emitter 结果发射器
   */
  @Override
  public void process(Pair<S, T> input, Emitter<Pair<T, S>> emitter) {
    // 用 first()/second() 取出原值，再按相反的顺序重新组成一个 Pair
    emitter.emit(Pair.of(input.second(), input.first()));
  }
}
