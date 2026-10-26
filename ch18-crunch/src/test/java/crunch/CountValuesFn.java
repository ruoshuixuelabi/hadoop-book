package crunch;

import java.util.Iterator;
import org.apache.crunch.MapFn;

/**
 * 统计一组元素个数的 MapFn
 *
 * <p>Crunch 的 {@code MapFn} 表示「一条记录进、一条记录出」的映射变换。
 * 这里把整个 {@code Iterable} 当成一条输入，输出它的元素个数，
 * 常配合 groupBy 之后统计每个分组里到底装了多少条记录。
 */
public class CountValuesFn<S> extends MapFn<Iterable<S>, Integer> {
  /**
   * 数出输入序列中一共有多少个元素
   *
   * @param input 待统计的元素序列
   * @return 元素个数；输入为空时返回 0
   */
  @Override
  public Integer map(Iterable<S> input) {
    // 计数器从 0 开始，每取到一个元素就加一
    int count = 0;
    // 这里用裸类型 Iterator：这个 MapFn 只关心「有多少个」
    // 而不关心元素本身是什么类型，多余的泛型信息用不上
    for (Iterator i = input.iterator(); i.hasNext(); ) {
      // 真正把元素取出来丢掉，map 的输出只与元素个数有关
      i.next();
      // 取到一个元素，计数加一
      count++;
    }
    // 返回统计结果，作为这条映射的输出
    return count;
  }
}
