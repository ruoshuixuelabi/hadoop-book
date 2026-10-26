import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.*;

/**
 * {@link NcdcStationMetadataParser} 的单元测试
 *
 * <p>覆盖正常记录、表头两种情况
 */
public class NcdcStationMetadataParserTest {
  
  // 被测对象
  private NcdcStationMetadataParser parser;

  /**
   * 每个测试方法之前都先建一个干净的解析器
   */
  @Before
  public void setUp() {
    // 解析器是有状态的，每次都要重新建
    parser = new NcdcStationMetadataParser();
  }
  
  /**
   * 解析一条正常的站记录
   */
  @Test
  public void parsesValidRecord() {
    // 一条正常的站记录
    assertThat(parser.parse("715390 99999 MOOSE JAW CS                  CN CA SA CZMJ  +50317 -105550 +05770"), is(true));
    // 站号
    assertThat(parser.getStationId(), is("715390-99999"));
    // 站名（两边有多余空格，所以要 trim）
    assertThat(parser.getStationName().trim(), is("MOOSE JAW CS"));
  }
  
  /**
   * 表头行不是有效的站记录，应该返回 false
   */
  @Test
  public void parsesHeader() {
    // 文件第一行是表头
    assertThat(parser.parse("Integrated Surface Database Station History, November 2007"), is(false));
  }
  
  /**
   * 空行也不是有效的站记录
   *
   * <p>注意这个方法上没有 {@code @Test} 注解，
   * 所以 JUnit 根本不会跑它——这是个容易被忽略的小坑
   */
  public void parsesBlankLine() {
    // 空行解析失败
    assertThat(parser.parse(""), is(false));
  }
  
}
