// == SingleResourceConfigurationTest
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * 验证 {@code Configuration} 只加载一个资源文件时的取值
 *
 * <p>重点看 {@code get} 的默认值语义：
 * 属性不存在时返回调用方给定的默认值，而不是抛异常
 */
public class SingleResourceConfigurationTest {
  
  /**
   * 验证取到已定义的属性、转换后的整数、以及默认值
   *
   * @throws IOException 读取资源文件失败
   */
  @Test
  public void get() throws IOException {
    // vv SingleResourceConfigurationTest
    // 建一个空对象
    Configuration conf = new Configuration();
    // 只登记这一个资源文件
    conf.addResource("configuration-1.xml");
    // 已定义的属性直接返回
    assertThat(conf.get("color"), is("yellow"));
    // getInt 在字符串取值的基础上再转成 int
    assertThat(conf.getInt("size", 0), is(10));
    // breadth 在配置文件里没有定义，于是返回调用方给的默认值 "wide"
    assertThat(conf.get("breadth", "wide"), is("wide"));
    // ^^ SingleResourceConfigurationTest
  }

}
