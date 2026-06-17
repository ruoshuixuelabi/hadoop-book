import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.*;

/**
 * NcdcStationMetadataParser 单元测试
 *
 * 测试内容：
 * - 解析有效的气象站元数据记录
 * - 提取站点 ID（USAF-WBAN 格式）和站点名称
 * - 识别并跳过表头行
 * - 处理空行
 *
 * ISD 数据格式：
 * USAF(6位) WBAN(5位) 站点名称(最多29字符) 国家代码等信息
 *
 * @since 4.0
 */
public class NcdcStationMetadataParserTest {

  /** 测试用的解析器实例 */
  private NcdcStationMetadataParser parser;

  /**
   * 测试前置 Setup 方法
   * 每个测试方法执行前都会创建新的解析器实例
   */
  @Before
  public void setUp() {
    parser = new NcdcStationMetadataParser();
  }

  /**
   * 测试：解析有效记录
   *
   * 验证解析器能正确解析完整的气象站元数据记录
   */
  @Test
  public void parsesValidRecord() {
    // 解析有效记录：加拿大 MOOSE JAW CS 气象站
    // 格式：715390(USAF) 99999(WBAN) MOOSE JAW CS(名称) CN CA SA CZMJ(国家) +50317 -105550 +05770(坐标)
    assertThat(parser.parse("715390 99999 MOOSE JAW CS                  CN CA SA CZMJ  +50317 -105550 +05770"), is(true));
    // 验证站点 ID 格式：USAF-WBAN
    assertThat(parser.getStationId(), is("715390-99999"));
    // 验证站点名称（去除前后空格）
    assertThat(parser.getStationName().trim(), is("MOOSE JAW CS"));
  }

  /**
   * 测试：解析表头行
   *
   * 验证解析器能正确识别并跳过 ISD 表头行
   */
  @Test
  public void parsesHeader() {
    // 解析 ISD 表头行
    assertThat(parser.parse("Integrated Surface Database Station History, November 2007"), is(false));
  }

  /**
   * 测试：解析空行
   *
   * 验证解析器能正确处理空行（返回 false）
   */
  public void parsesBlankLine() {
    // 解析空行
    assertThat(parser.parse(""), is(false));
  }

}
