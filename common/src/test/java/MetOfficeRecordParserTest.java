import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.*;

/**
 * {@link MetOfficeRecordParser} 的单元测试
 *
 * <p>覆盖正常记录、负温度、温度缺失、表头、格式错误五种情况
 */
public class MetOfficeRecordParserTest {
  
  // 被测对象
  private MetOfficeRecordParser parser;

  /**
   * 每个测试方法之前都先建一个干净的解析器
   */
  @Before
  public void setUp() {
    // 解析器是有状态的，每次都要重新建
    parser = new MetOfficeRecordParser();
  }
  
  /**
   * 解析一条正常的记录
   */
  @Test
  public void parsesValidRecord() {
    // 温度是 7.5 度
    parser.parse("   1978   1    7.5     2.0       6   134.1    64.7");
    // 年份
    assertThat(parser.getYear(), is("1978"));
    // 温度乘以 10 之后是 75
    assertThat(parser.getAirTemperature(), is(75));
    // 温度字段的原始写法（保留了前面的空格）
    assertThat(parser.getAirTemperatureString(), is("  7.5"));
    // 温度可用
    assertThat(parser.isValidTemperature(), is(true));
  }
  
  /**
   * 解析一条负温度的记录
   */
  @Test
  public void parsesNegativeTemperature() {
    // 温度是 -17.5 度
    parser.parse("   1978   1  -17.5     2.0       6   134.1    64.7");
    // 年份
    assertThat(parser.getYear(), is("1978"));
    // 温度乘以 10 之后是 -175
    assertThat(parser.getAirTemperature(), is(-175));
    // 温度字段的原始写法
    assertThat(parser.getAirTemperatureString(), is("-17.5"));
    // 负温度也是有效温度
    assertThat(parser.isValidTemperature(), is(true));
  }
  
  /**
   * 温度缺失时算不出温度
   */
  @Test
  public void parsesMissingTemperature() {
    // 温度字段是 ---
    parser.parse("   1853   1    ---     ---     ---    57.3     ---");
    // 原始写法
    assertThat(parser.getAirTemperatureString(), is("  ---"));
    // 所以温度不可用
    assertThat(parser.isValidTemperature(), is(false));
  }
  
  /**
   * 表头行不产生有效温度
   */
  @Test
  public void parsesHeaderLine() {
    // 文件开头有若干行表头
    parser.parse("Cardiff Bute Park");
    // 表头不是数据行
    assertThat(parser.isValidTemperature(), is(false));
  }
  
  /**
   * 温度格式不对时应该抛异常
   */
  @Test(expected=NumberFormatException.class)
  public void cannotParseMalformedTemperature() {
    // 温度字段是 X.5，解析不了
    parser.parse("   1978   1    X.5     2.0       6   134.1    64.7");
  }
}
