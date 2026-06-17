import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * BinaryOrTextWritable - 支持 BytesWritable 或 Text 的泛型 Writable
 *
 * <p>此类继承 GenericWritable,能够存储两种不同 Writable 类型之一的值。
 * 在需要处理多种数据类型时很有用,例如某些字段可能是文本或二进制数据。</p>
 *
 * <p><b>支持的类型:</b></p>
 * <ul>
 *   <li>BytesWritable: 二进制数据</li>
 *   <li>Text: UTF-8 文本数据</li>
 * </ul>
 *
 * <p><b>使用场景:</b></p>
 * <ul>
 *   <li>动态类型的值字段</li>
 *   <li>协议缓冲区中的 Any 类型</li>
 *   <li>需要兼容多种数据格式的场景</li>
 * </ul>
 *
 * @see GenericWritable
 * @see BytesWritable
 * @see Text
 * @see GenericWritableTest
 */
public class BinaryOrTextWritable extends GenericWritable {

    /**
     * 支持的 Writable 类型数组
     *
     * <p>GenericWritable 需要知道它可能包含哪些类型,
     * 以便在反序列化时正确创建实例。</p>
     */
    private static Class[] TYPES = { BytesWritable.class, Text.class };

    /**
     * 返回此 GenericWritable 支持的 Writable 类型数组
     *
     * <p>此方法由 GenericWritable 框架在序列化和反序列化时调用,
     * 用于确定需要实例化哪个具体的 Writable 子类。</p>
     *
     * @return 支持的 Writable 类型数组
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Class<? extends Writable>[] getTypes() {
        return TYPES;
    }

}
