package crunch;

import java.util.Iterator;
import org.apache.crunch.MapFn;

/**
 * 计数函数
 *
 * 用于计算Iterable中元素的数量。
 * 这是一个MapFn,将Iterable输入转换为Integer输出。
 *
 * 使用场景:
 * - 统计每个键对应的值数量
 * - 生成直方图
 *
 * @param <S> 输入迭代器的元素类型
 */
public class CountValuesFn<S> extends MapFn<Iterable<S>, Integer> {

    /**
     * 计算Iterable中的元素数量
     *
     * @param input 元素迭代器
     * @return 元素数量
     */
    @Override
    public Integer map(Iterable<S> input) {
        int count = 0;
        for (Iterator i = input.iterator(); i.hasNext(); ) {
            i.next();
            count++;
        }
        return count;
    }
}
