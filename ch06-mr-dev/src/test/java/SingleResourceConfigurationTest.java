// == SingleResourceConfigurationTest

/**
 * 单资源配置测试
 *
 * 测试Configuration.addResource()方法加载单个配置文件的功能。
 *
 * 测试验证点：
 * 1. get("color") - 获取配置值（应为"yellow"）
 * 2. getInt("size", 0) - 获取整型配置值（应为10）
 * 3. get("breadth", "wide") - 获取配置值，如不存在则使用默认值（应为"wide"）
 *
 * 测试文件：configuration-1.xml
 *
 * @see org.apache.hadoop.conf.Configuration
 */
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * 单资源配置测试类
 */
public class SingleResourceConfigurationTest {

  /**
   * 测试加载单个资源文件
   *
   * 测试流程：
   * 1. 创建空的Configuration
   * 2. 添加configuration-1.xml资源
   * 3. 验证各项配置值
   *
   * @throws IOException IO异常（如文件不存在）
   */
  @Test
  public void get() throws IOException {
    // vv SingleResourceConfigurationTest
    // 创建Configuration实例
    Configuration conf = new Configuration();
    // 添加资源文件
    conf.addResource("configuration-1.xml");
    // 验证字符串配置
    assertThat(conf.get("color"), is("yellow"));
    // 验证整型配置（默认值0）
    assertThat(conf.getInt("size", 0), is(10));
    // 验证字符串配置，使用默认值"wide"
    assertThat(conf.get("breadth", "wide"), is("wide"));
    // ^^ SingleResourceConfigurationTest
  }

}