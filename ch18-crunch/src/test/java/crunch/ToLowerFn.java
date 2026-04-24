package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

/**
 * 转小写函数
 *
 * 将输入字符串转换为小写。
 *
 * 使用场景:
 * - 文本规范化
 * - 不区分大小写的匹配
 */
public class ToLowerFn extends DoFn<String, String> {

    /**
     * 将输入字符串转为小写
     *
     * @param input 输入字符串
     * @param emitter 发射器
     */
    @Override
    public void process(String input, Emitter<String> emitter) {
        emitter.emit(input.toLowerCase());
    }
}