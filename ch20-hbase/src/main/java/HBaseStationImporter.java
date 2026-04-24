import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 将气象站点元数据导入 HBase 的 "stations" 表
 *
 * 此导入器读取气象站点的元数据文件(包含站点ID到名称的映射),
 * 并将每条记录作为一行写入 HBase 表。
 *
 * 数据模型:
 * - 表名: stations
 * - 行键: 站点ID (原始 12 字符 ID)
 * - 列族: "info"
 * - 列:
 *   - info:name: 站点名称
 *   - info:description: 站点描述 (此处设为 "(unknown)")
 *   - info:location: 站点位置 (此处设为 "(unknown)")
 *
 * /*[*
 * stations 表存储气象站点的静态元数据,
 * 与存储实时观测数据的 observations 表配合使用。
 * 查询时可根据站点ID获取站点信息,再查询观测数据。
 * *]*/
public class HBaseStationImporter extends Configured implements Tool {

  /**
   * 执行站点数据导入
   *
   * 流程:
   * 1. 解析命令行参数,获取元数据文件路径
   * 2. 创建 HTable 实例连接到 "stations" 表
   * 3. 读取并解析站点元数据文件
   * 4. 为每个站点创建 Put 操作并执行写入
   * 5. 关闭表连接
   *
   * @param args 命令行参数, args[0] 为站点元数据文件路径
   * @return 执行状态 (0 成功, -1 失败)
   */
  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseStationImporter <input>");
      return -1;
    }

    // 创建 HTable 实例连接到 "stations" 表
    // HBaseConfiguration.create(getConf()) 使用作业配置创建 HBase 配置
    HTable table = new HTable(HBaseConfiguration.create(getConf()), "stations");
    try {
      // 创建站点元数据解析器
      NcdcStationMetadata metadata = new NcdcStationMetadata();
      // 初始化解析器,传入元数据文件
      metadata.initialize(new File(args[0]));

      // 获取站点ID到名称的映射
      Map<String, String> stationIdToNameMap = metadata.getStationIdToNameMap();

      // 遍历每个站点,创建 Put 并写入 HBase
      for (Map.Entry<String, String> entry : stationIdToNameMap.entrySet()) {
        // 使用站点ID作为行键
        Put put = new Put(Bytes.toBytes(entry.getKey()));

        // 添加站点名称列: info:name
        put.add(HBaseStationQuery.INFO_COLUMNFAMILY, HBaseStationQuery.NAME_QUALIFIER,
            Bytes.toBytes(entry.getValue()));

        // 添加站点描述列: info:description (设为 "(unknown)",表示未知)
        put.add(HBaseStationQuery.INFO_COLUMNFAMILY, HBaseStationQuery.DESCRIPTION_QUALIFIER,
            Bytes.toBytes("(unknown)"));

        // 添加站点位置列: info:location (设为 "(unknown)",表示未知)
        put.add(HBaseStationQuery.INFO_COLUMNFAMILY, HBaseStationQuery.LOCATION_QUALIFIER,
            Bytes.toBytes("(unknown)"));

        // 执行写入
        table.put(put);
      }
      return 0;
    } finally {
      // 确保 HTable 关闭,释放连接资源
      table.close();
    }
  }

  /**
   * 程序入口: 使用 ToolRunner 运行导入工具
   *
   * ToolRunner 负责配置管理和异常处理
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new HBaseStationImporter(), args);
    System.exit(exitCode);
  }
}