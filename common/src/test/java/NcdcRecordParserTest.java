import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.*;

/**
 * NcdcRecordParser 单元测试
 *
 * 测试内容：
 * - 解析有效的 NCDC 气象记录
 * - 提取站点 ID、观测年份、气温、质量代码等字段
 * - 验证气温数据的有效性判断
 * - 处理缺失温度记录
 * - 处理格式错误的气温值
 *
 * NCDC 数据格式：
 * 固定长度文本行，包含站点 ID、观测时间、气温、质量代码等信息
 *
 * @since 4.0
 */
public class NcdcRecordParserTest {

  /** 测试用的解析器实例 */
  private NcdcRecordParser parser;

  /**
   * 测试前置 Setup 方法
   * 每个测试方法执行前都会创建新的解析器实例
   */
  @Before
  public void setUp() {
    parser = new NcdcRecordParser();
  }

  /**
   * 测试：解析有效记录
   *
   * 验证解析器能正确解析完整的有效 NCDC 记录
   */
  @Test
  public void parsesValidRecord() {
    // 解析有效记录
    // 格式：0043011990999991950051512004+68750+023550FM-12+038299999V0203201N00671220001CN9999999N9+00221+99999999999
    parser.parse("0043011990999991950051512004+68750+023550FM-12+038299999V0203201N00671220001CN9999999N9+00221+99999999999");
    // 验证站点 ID 解析正确：011990-99999
    assertThat(parser.getStationId(), is("011990-99999"));
    // 验证年份解析正确：1950
    assertThat(parser.getYear(), is("1950"));
    // 验证气温解析正确：0022 -> 22（摄氏度 * 10）
    assertThat(parser.getAirTemperature(), is(22));
    // 验证气温原始字符串
    assertThat(parser.getAirTemperatureString(), is("0022"));
    // 验证气温标记为有效
    assertThat(parser.isValidTemperature(), is(true));
    // 验证质量代码：1 = 通过质量检查
    assertThat(parser.getQuality(), is("1"));
  }

  /**
   * 测试：解析缺失温度记录
   *
   * 验证解析器能正确识别缺失的气温数据（9999）
   */
  @Test
  public void parsesMissingTemperature() {
    // 解析缺失温度记录（气温部分为 +99991）
    parser.parse("0043011990999991950051512004+68750+023550FM-12+038299999V0203201N00671220001CN9999999N9+99991+99999999999");
    // 验证气温值为缺失标记 9999
    assertThat(parser.getAirTemperature(), is(9999));
    // 验证气温原始字符串
    assertThat(parser.getAirTemperatureString(), is("9999"));
    // 验证气温标记为无效（缺失数据）
    assertThat(parser.isValidTemperature(), is(false));
  }

  /**
   * 测试：无法解析格式错误的气温
   *
   * 验证解析器在遇到非数字气温时会抛出 NumberFormatException
   */
  @Test(expected=NumberFormatException.class)
  public void cannotParseMalformedTemperature() {
    // 解析包含非数字气温的记录（如 +XXXX1）
    parser.parse("0043011990999991950051512004+68750+023550FM-12+038299999V0203201N00671220001CN9999999N9+XXXX1+99999999999");
  }
}
