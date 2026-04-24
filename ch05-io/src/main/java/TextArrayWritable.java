// == TextArrayWritable
// cc TextArrayWritable 一个专门用于存储 Text 对象数组的 ArrayWritable 子类

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

// vv TextArrayWritable
/**
 * TextArrayWritable - Text 对象数组的 Writable 实现
 *
 * <p>ArrayWritable 是 Hadoop 中用于表示数组类型的 Writable 抽象类。
 * 此类将数组元素类型固定为 Text,提供更具体类型安全性和便捷的构造方法。</p>
 *
 * <p><b>主要用途:</b></p>
 * <ul>
 *   <li>在 MapReduce 中作为值类型传输字符串数组</li>
 *   <li>替代 Java 的 String[] 数组,利用 Hadoop 序列化机制</li>
 * </ul>
 *
 * <p><b>示例:</b></p>
 * <pre>{@code
 * TextArrayWritable array = new TextArrayWritable();
 * array.set(new Text[] { new Text("hello"), new Text("world") });
 * Text[] texts = (Text[]) array.toArray();
 * }</pre>
 *
 * @see ArrayWritable
 * @see Text
 */
public class TextArrayWritable extends ArrayWritable {

    /**
     * 默认构造函数
     *
     * <p>将数组元素类型设置为 Text.class。
     * 所有存储在此数组中的元素都必须是 Text 类型或其子类。</p>
     */
    public TextArrayWritable() {
        // 调用父类构造函数,指定数组元素类型为 Text
        super(Text.class);
    }
}
// ^^ TextArrayWritable
