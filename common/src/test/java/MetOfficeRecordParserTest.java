import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.*;

/**
 * MetOfficeRecordParser 单元测试
 *
 * 测试内容：
 * - 解析有效的天气记录
 * - 解析负温度记录
 * - 解析缺失温度记录（---）
 * - 解析表头行
 * - 处理格式错误的气温值
 *
 * 测试数据格式（Met Office）：
 * 空格分隔的文本行，包含年份和气温等信息
 *
 * @since 4.0
 */
public class MetOfficeRecordParserTest {

  /** 测试用的解析器实例 */
  private MetOfficeRecordParser parser;

  /**
   * 测试前置 Setup 方法
   * 每个测试方法执行前都会创建新的解析器实例
   */
  @Before
  public void setUp() {
    parser = new MetOfficeRecordParser();
  }

  /**
   * 测试：解析有效记录
   *
   * 验证解析器能正确解析包含正温度的有效记录
   */
  @Test
  public void parsesValidRecord() {
    // 解析有效记录：年份 1978，气温 7.5
    parser.parse("   1978   1    7.5     2.0       6   134.1    64.7");
    // 验证年份解析正确
    assertThat(parser.getYear(), is("1978"));
    // 验证气温解析正确（7.5 * 10 = 75）
    assertThat(parser.getAirTemperature(), is(75));
    // 验证气温原始字符串
    assertThat(parser.getAirTemperatureString(), is("  7.5"));
    // 验证气温标记为有效
    assertThat(parser.isValidTemperature(), is(true));
  }

  /**
   * 测试：解析负温度记录
   *
   * 验证解析器能正确处理负温度值
   */
  @Test
  public void parsesNegativeTemperature() {
    // 解析负温度记录：气温 -17.5
    parser.parse("   1978   1  -17.5     2.0       6   134.1    64.7");
    // 验证年份解析正确
    assertThat(parser.getYear(), is("1978"));
    // 验证负气温解析正确（-17.5 * 10 = -175）
    assertThat(parser.getAirTemperature(), is(-175));
    // 验证气温原始字符串（包含负号）
    assertThat(parser.getAirTemperatureString(), is("-17.5"));
    // 验证气温标记为有效
    assertThat(parser.isValidTemperature(), is(true));
  }

  /**
   * 测试：解析缺失温度记录
   *
   * 验证解析器能正确识别缺失的气温数据（---）
   */
  @Test
  public void parsesMissingTemperature() {
    // 解析缺失温度记录：气温显示为 ---
    parser.parse("   1853   1    ---     ---     ---    57.3     ---");
    // 验证气温原始字符串为 ---
    assertThat(parser.getAirTemperatureString(), is("  ---"));
    // 验证气温标记为无效
    assertThat(parser.isValidTemperature(), is(false));
  }

  /**
   * 测试：解析表头行
   *
   * 验证解析器能正确识别并跳过表头行（不是有效记录）
   */
  @Test
  public void parsesHeaderLine() {
    // 解析表头行（如 "Cardiff Bute Park"）
    parser.parse("Cardiff Bute Park");
    // 验证气温标记为无效（表头不是有效数据）
    assertThat(parser.isValidTemperature(), is(false));
  }

  /**
   * 测试：无法解析格式错误的气温
   *
   * 验证解析器在遇到非数字气温时会抛出 NumberFormatException
   */
  @Test(expected=NumberFormatException.class)
  public void cannotParseMalformedTemperature() {
    // 解析包含非数字气温的记录（如 "X.5"）
    parser.parse("   1978   1    X.5     2.0       6   134.1    64.7");
  }
}
