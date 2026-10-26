package oldapi;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IOUtils;

/**
 * 气象站元数据：站号到站名的映射表（老 API 版）
 *
 * <p>这份是 {@code common} 模块里给「旧版 {@code mapred} API」准备的副本，
 * 放在 {@code oldapi} 包下，逻辑和新 API 版完全一样
 * <p>把 {@code stations.txt} 这样的元数据文件读进来，
 * 之后就能拿站号直接查到站名
 */
public class NcdcStationMetadata {
  
  // 站号 → 站名
  private Map<String, String> stationIdToName = new HashMap<String, String>();

  /**
   * 从元数据文件里读出所有站号和站名
   *
   * @param file 元数据文件
   * @throws IOException 读文件失败
   */
  public void initialize(File file) throws IOException {
    BufferedReader in = null;
    try {
      // 逐行读取元数据文件
      in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
      // 解析器复用，一行一行地喂给它
      NcdcStationMetadataParser parser = new NcdcStationMetadataParser();
      String line;
      // 文件读到完为止
      while ((line = in.readLine()) != null) {
        // 表头之类的行会被解析器拒掉
        if (parser.parse(line)) {
          // 站号和站名配成一对存进去
          stationIdToName.put(parser.getStationId(), parser.getStationName());
        }
      }
    } finally {
      // 无论正常结束还是抛异常，文件都要关掉
      IOUtils.closeStream(in);
    }
  }

  /**
   * 按站号查站名
   *
   * <p>查不到就退回用站号本身，
   * 这样调用方拿到的名字永远不会是 {@code null}
   *
   * @param stationId 站号
   * @return 站名；查不到时返回站号本身
   */
  public String getStationName(String stationId) {
    // 先查一下
    String stationName = stationIdToName.get(stationId);
    // 没查到，或者查到的名字是空的
    if (stationName == null || stationName.trim().length() == 0) {
      // 退回用站号顶上
      return stationId; // no match: fall back to ID
    }
    return stationName;
  }
  
  /**
   * 取整张站号到站名的映射表
   *
   * <p>返回的是不可修改的视图，外面改不了里面的内容
   *
   * @return 站号到站名的映射表
   */
  public Map<String, String> getStationIdToNameMap() {
    // 包一层，防止调用方把内部状态改坏
    return Collections.unmodifiableMap(stationIdToName);
  }
  
}
