// == TextArrayWritable
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

// vv TextArrayWritable
/**
 * 用 {@link Text} 作为元素类型的数组
 *
 * <p>{@link org.apache.hadoop.io.ArrayWritable} 是 Hadoop 自带的数组实现：
 * 它把同类型的元素依次序列化，元素类型由构造器指定。
 * 子类只要在构造器里调用 {@code super(Text.class)} 即可，
 * 之后就能像操作普通数组一样使用它
 */
public class TextArrayWritable extends ArrayWritable {
  /**
   * 构造一个元素类型为 {@link Text} 的数组
   *
   * <p>把元素类型传给父类，是 {@link org.apache.hadoop.io.ArrayWritable}
   * 反射创建元素实例所必需的
   */
  public TextArrayWritable() {
    // 元素类型固定为 Text，父类会据此反射创建每个元素
    super(Text.class);
  }
}
// ^^ TextArrayWritable
