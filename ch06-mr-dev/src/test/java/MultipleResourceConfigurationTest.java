// == MultipleResourceConfigurationTest
// == MultipleResourceConfigurationTest-Override
// == MultipleResourceConfigurationTest-Final
// == MultipleResourceConfigurationTest-Expansion
// == MultipleResourceConfigurationTest-SystemExpansion
// == MultipleResourceConfigurationTest-NoSystemByDefault
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * 验证 {@code Configuration} 加载多个资源文件时的各种行为
 *
 * <p>覆盖五条规则：
 * <ul>
 * <li>后加载的资源覆盖先加载的（size 从 10 变成 12）</li>
 * <li>被标记为 final 的属性不可被覆盖（weight 仍是 heavy）</li>
 * <li>属性值支持变量展开（${size},${weight}）</li>
 * <li>展开时优先使用系统属性</li>
 * <li>默认不读取系统属性本身</li>
 * </ul>
 */
public class MultipleResourceConfigurationTest {
  
  /**
   * 依次验证上述五条规则
   *
   * @throws IOException 读取资源文件失败
   */
  @Test
  public void get() throws IOException {
    // Single test as an expedient for inclusion in the book
    
    // vv MultipleResourceConfigurationTest
    // 先建一个空对象，再逐个添加资源文件
    Configuration conf = new Configuration();
    // addResource 只是「登记」文件名，真正解析在第一次 get 时才发生
    conf.addResource("configuration-1.xml");
    // 后添加的资源优先级更高
    conf.addResource("configuration-2.xml");
    // ^^ MultipleResourceConfigurationTest
    
    // color 只在 configuration-1.xml 里定义，所以取到 yellow
    assertThat(conf.get("color"), is("yellow"));

    // override
    // vv MultipleResourceConfigurationTest-Override
    // size 在两个文件里都有：configuration-1 是 10，configuration-2 是 12，
    // 后者覆盖前者，所以结果是 12
    assertThat(conf.getInt("size", 0), is(12));
    // ^^ MultipleResourceConfigurationTest-Override

    // final properties cannot be overridden
    // vv MultipleResourceConfigurationTest-Final
    // weight 在 configuration-1.xml 里被标成 final，
    // 因此 configuration-2.xml 里的 light 覆盖不了它
    assertThat(conf.get("weight"), is("heavy"));
    // ^^ MultipleResourceConfigurationTest-Final

    // variable expansion
    // vv MultipleResourceConfigurationTest-Expansion
    // 变量展开：${size} 用当前生效的 12，${weight} 用 heavy
    assertThat(conf.get("size-weight"), is("12,heavy"));
    // ^^ MultipleResourceConfigurationTest-Expansion

    // variable expansion with system properties
    // vv MultipleResourceConfigurationTest-SystemExpansion
    // 改一下系统属性，再展开时就会优先用系统属性的值
    System.setProperty("size", "14");
    // 所以 ${size} 变成了 14
    assertThat(conf.get("size-weight"), is("14,heavy"));
    // ^^ MultipleResourceConfigurationTest-SystemExpansion

    // system properties are not picked up
    // vv MultipleResourceConfigurationTest-NoSystemByDefault
    // 但 Configuration 默认不读系统属性
    System.setProperty("length", "2");
    // 因此取到的是 null，而不是 "2"
    assertThat(conf.get("length"), is((String) null));
    // ^^ MultipleResourceConfigurationTest-NoSystemByDefault

  }

}
