import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 一个「既能装二进制又能装文本」的 {@link org.apache.hadoop.io.GenericWritable}
 *
 * <p>{@code GenericWritable} 可以装任意 {@code Writable}，
 * 代价是它只按类型编号（而不是按内容）来序列化，
 * 所以必须告诉它「我可能会装哪几种类型」——
 * 这正是 {@link #getTypes()} 的作用
 * <p>本类把候选类型限定为 {@code BytesWritable} 与 {@code Text} 两种
 */
public class BinaryOrTextWritable extends GenericWritable {
  /** 允许被装进本对象的类型清单，序号即序列化时写入的类型编号 */
  private static Class[] TYPES = { BytesWritable.class, Text.class };

  /**
   * 声明本对象可能承载的 Writable 类型
   *
   * <p>父类 {@link org.apache.hadoop.io.GenericWritable} 在序列化时
   * 会查出元素的实际类型，再把这个类型在数组里的下标写出去；
   * 反序列化时再按下标找回类型
   * <p>因此这个数组的顺序一旦确定就不能改，
   * 否则历史数据将无法正确还原
   *
   * @return 允许的类型数组
   */
  @Override
  @SuppressWarnings("unchecked")
  protected Class<? extends Writable>[] getTypes() {
    return TYPES;
  }
  
}
