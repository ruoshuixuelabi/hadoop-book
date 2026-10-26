import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.*;

/**
 * {@link NcdcRecordParser} 的单元测试
 *
 * <p>覆盖三种情况：正常的记录、温度缺失（9999）、温度字段格式不对
 */
public class NcdcRecordParserTest {
  
  // 被测对象
  private NcdcRecordParser parser;

  /**
   * 每个测试方法之前都先建一个干净的解析器
   */
  @Before
  public void setUp() {
    // 解析器是有状态的，每次都要重新建
    parser = new NcdcRecordParser();
  }
  
  /**
   * 解析一条正常的记录，各字段都应该对
   */
  @Test
  public void parsesValidRecord() {
    // 一条温度为 22 度的正常记录
    parser.parse("0043011990999991950051512004+68750+023550FM-12+038299999V0203201N00671220001CN9999999N9+00221+99999999999");
    // 站号
    assertThat(parser.getStationId(), is("011990-99999"));
    // 年份
    assertThat(parser.getYear(), is("1950"));
    // 温度
    assertThat(parser.getAirTemperature(), is(22));
    // 温度字段的原始写法
    assertThat(parser.getAirTemperatureString(), is("0022"));
    // 这条记录的温度是可用的
    assertThat(parser.isValidTemperature(), is(true));
    // 质量标记
    assertThat(parser.getQuality(), is("1"));
  }
  
  /**
   * 解析一条温度缺失的记录
   */
  @Test
  public void parsesMissingTemperature() {
    // 温度字段是 9999，表示没测到
    parser.parse("0043011990999991950051512004+68750+023550FM-12+038299999V0203201N00671220001CN9999999N9+99991+99999999999");
    // 温度就是缺失值 9999
    assertThat(parser.getAirTemperature(), is(9999));
    // 原始写法也是 9999
    assertThat(parser.getAirTemperatureString(), is("9999"));
    // 所以这条记录的温度不可用
    assertThat(parser.isValidTemperature(), is(false));
  }
  
  /**
   * 温度字段格式不对时应该抛异常
   */
  @Test(expected=NumberFormatException.class)
  public void cannotParseMalformedTemperature() {
    // 温度字段是 XXXX，解析不了
    parser.parse("0043011990999991950051512004+68750+023550FM-12+038299999V0203201N00671220001CN9999999N9+XXXX1+99999999999");
  }
}
