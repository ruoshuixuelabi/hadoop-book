// == MapWritableTest
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

/**
 * 验证 {@link org.apache.hadoop.io.MapWritable} 的键值存放能力
 *
 * <p>{@code MapWritable} 是「键也是 Writable」的哈希表：
 * 键自身要能序列化，所以 {@code put} 进去的必须是
 * {@code Writable} 而不是普通对象
 * <p>另外它支持用 {@link org.apache.hadoop.io.NullWritable} 当键，
 * 便于实现「一个值只出现一次」这类集合语义
 */
public class MapWritableTest extends WritableTestBase {
  
  /**
   * 验证不同键类型的元素能被正确存入并克隆回来
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void mapWritable() throws IOException {
    // vv MapWritableTest
    MapWritable src = new MapWritable();
    // 键是 IntWritable，值是 Text
    src.put(new IntWritable(1), new Text("cat"));
    // 换一个键类型，验证不同类型可以共存
    src.put(new VIntWritable(2), new LongWritable(163));
    
    MapWritable dest = new MapWritable();
    // 克隆时要连键的类型信息一起写入
    WritableUtils.cloneInto(dest, src);
    assertThat((Text) dest.get(new IntWritable(1)), is(new Text("cat")));
    // 取值时必须用「类型完全一致」的键才能命中
    assertThat((LongWritable) dest.get(new VIntWritable(2)),
        is(new LongWritable(163)));
    // ^^ MapWritableTest
  }

  /**
   * 验证用 {@code NullWritable} 作键可以实现「集合」语义
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void setWritableEmulation() throws IOException {
    MapWritable src = new MapWritable();
    // NullWritable 自身不携带数据，只占一个「存在」的标记
    src.put(new IntWritable(1), NullWritable.get());
    src.put(new IntWritable(2), NullWritable.get());
    
    MapWritable dest = new MapWritable();
    WritableUtils.cloneInto(dest, src);
    // 用途：把若干个 key 收进一个「集合」，重复插入不影响结果
    assertThat(dest.containsKey(new IntWritable(1)), is(true));
  }
}
