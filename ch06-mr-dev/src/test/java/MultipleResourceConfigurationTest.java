// == MultipleResourceConfigurationTest
// == MultipleResourceConfigurationTest-Override
// == MultipleResourceConfigurationTest-Final
// == MultipleResourceConfigurationTest-Expansion
// == MultipleResourceConfigurationTest-SystemExpansion
// == MultipleResourceConfigurationTest-NoSystemByDefault

/**
 * 多资源配置测试
 *
 * 测试加载多个配置文件时的覆盖、变量展开和系统属性行为。
 *
 * 测试文件：
 * - configuration-1.xml - 基础配置
 * - configuration-2.xml - 覆盖配置
 *
 * 测试场景：
 * 1. 基础属性读取 - color应为"yellow"（来自configuration-1.xml）
 * 2. 属性覆盖 - size应被覆盖为12（来自configuration-2.xml）
 * 3. Final属性 - weight="heavy"不可被覆盖
 * 4. 变量展开 - size-weight应为"12,heavy"
 * 5. 系统属性展开 - size-weight使用系统属性"14"
 * 6. 系统属性默认 - length不自动读取系统属性
 *
 * @see org.apache.hadoop.conf.Configuration
 */
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * 多资源配置测试类
 */
public class MultipleResourceConfigurationTest {

  /**
   * 测试多资源配置的各种特性
   *
   * @throws IOException IO异常
   */
  @Test
  public void get() throws IOException {
    // Single test as an expedient for inclusion in the book

    // vv MultipleResourceConfigurationTest
    // 创建Configuration并加载两个资源文件
    Configuration conf = new Configuration();
    conf.addResource("configuration-1.xml");
    conf.addResource("configuration-2.xml");
    // ^^ MultipleResourceConfigurationTest

    // 验证基础属性（未被覆盖）
    assertThat(conf.get("color"), is("yellow"));

    // 测试属性覆盖功能
    // vv MultipleResourceConfigurationTest-Override
    // size在configuration-2.xml中被覆盖为12
    assertThat(conf.getInt("size", 0), is(12));
    // ^^ MultipleResourceConfigurationTest-Override

    // 测试final属性（不能被覆盖）
    // vv MultipleResourceConfigurationTest-Final
    assertThat(conf.get("weight"), is("heavy"));
    // ^^ MultipleResourceConfigurationTest-Final

    // 测试变量展开功能
    // vv MultipleResourceConfigurationTest-Expansion
    // ${size}和${weight}被展开为实际值
    assertThat(conf.get("size-weight"), is("12,heavy"));
    // ^^ MultipleResourceConfigurationTest-Expansion

    // 测试系统属性在变量展开中的作用
    // vv MultipleResourceConfigurationTest-SystemExpansion
    // 设置系统属性后，${size}使用系统值"14"
    System.setProperty("size", "14");
    assertThat(conf.get("size-weight"), is("14,heavy"));
    // ^^ MultipleResourceConfigurationTest-SystemExpansion

    // 测试系统属性默认不自动读取
    // vv MultipleResourceConfigurationTest-NoSystemByDefault
    // 设置系统属性length=2，但Configuration不会自动读取它
    System.setProperty("length", "2");
    assertThat(conf.get("length"), is((String) null));
    // ^^ MultipleResourceConfigurationTest-NoSystemByDefault

  }

}