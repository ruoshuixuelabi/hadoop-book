import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IOUtils;

/**
 * NCDC 气象站元数据管理器
 *
 * 功能说明：
 * - 从外部文件加载气象站元数据
 * - 提供站点 ID 到站点名称的映射查询
 * - 支持获取完整的站点映射表
 *
 * 数据来源：
 * - ISD（Integrated Surface Database）气象站历史数据
 * - 文件格式：USAF-WBAN ID、站点名称、国家代码等信息
 *
 * 使用场景：
 * - MapReduce 作业中将站点 ID 转换为可读名称
 * - 数据清洗和可视化时显示站点名称
 *
 * @since 4.0
 */
public class NcdcStationMetadata {

  /** 站点 ID（格式：USAF-WBAN）到站点名称的映射 */
  private Map<String, String> stationIdToName = new HashMap<String, String>();

  /**
   * 从文件初始化气象站元数据
   *
   * 读取 ISD 气象站历史数据文件，逐行解析并构建站点映射表。
   * 文件格式示例：
   * 715390 99999 MOOSE JAW CS CN CA SA CZMJ +50317 -105550 +05770
   *
   * @param file 气象站元数据文件
   * @throws IOException 如果文件读取失败
   */
  public void initialize(File file) throws IOException {
    BufferedReader in = null;
    try {
      // 创建缓冲字符输入流，以 UTF-8 编码读取文件
      in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
      // 创建解析器实例（复用同一个解析器对象）
      NcdcStationMetadataParser parser = new NcdcStationMetadataParser();
      String line;

      // 逐行读取文件内容
      while ((line = in.readLine()) != null) {
        // 解析每行数据，成功解析才添加到映射表
        if (parser.parse(line)) {
          // 格式：USAF-WBAN -> 站点名称
          stationIdToName.put(parser.getStationId(), parser.getStationName());
        }
      }
    } finally {
      // 确保输入流被关闭，释放系统资源
      IOUtils.closeStream(in);
    }
  }

  /**
   * 根据站点 ID 获取站点名称
   *
   * 如果找不到对应的站点名称，则返回站点 ID 作为后备值。
   *
   * @param stationId 站点 ID，格式：USAF-WBAN（如 "011990-99999"）
   * @return 站点名称，如果未找到则返回站点 ID
   */
  public String getStationName(String stationId) {
    String stationName = stationIdToName.get(stationId);
    // 无匹配记录时，返回站点 ID 作为后备
    if (stationName == null || stationName.trim().length() == 0) {
      return stationId;
    }
    return stationName;
  }

  /**
   * 获取完整的站点 ID 到名称映射表
   *
   * 返回的映射表是只读的，防止外部修改导致数据不一致。
   *
   * @return 不可修改的站点映射表
   */
  public Map<String, String> getStationIdToNameMap() {
    // 使用 Collections.unmodifiableMap 创建只读视图
    return Collections.unmodifiableMap(stationIdToName);
  }

}
