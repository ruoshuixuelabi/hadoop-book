import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HBase 1.0+ 版本的气象站点查询器
 *
 * 升级到 HBase 2.6.4 版本,使用 Connection/Table API 替代旧的 HTable。
 *
 * 功能: 根据站点ID查询 stations 表,获取站点信息:
 * - name (站点名称)
 * - location (站点位置)
 * - description (站点描述)
 *
 * /*[*
 * HBase 2.x 中推荐的 API 使用模式:
 * - Connection 是重量级对象,应复用,可共享给多个 Table
 * - Table 是轻量级对象,用完需关闭
 * - 不推荐使用旧的 HTable,已被废弃
 * *]*/
public class NewHBaseStationQuery extends Configured implements Tool {
  // "info" 列族的字节数组表示
  static final byte[] INFO_COLUMNFAMILY = Bytes.toBytes("info");
  // "name" 列限定符的字节数组
  static final byte[] NAME_QUALIFIER = Bytes.toBytes("name");
  // "location" 列限定符的字节数组
  static final byte[] LOCATION_QUALIFIER = Bytes.toBytes("location");
  // "description" 列限定符的字节数组
  static final byte[] DESCRIPTION_QUALIFIER = Bytes.toBytes("description");

  /**
   * 获取站点信息
   *
   * 使用 Table.get() 执行单行查询。
   *
   * @param table Table 接口实例
   * @param stationId 站点ID (行键)
   * @return 站点信息 Map,若不存在则返回 null
   * @throws IOException IO 错误时抛出
   */
  public Map<String, String> getStationInfo(Table table, String stationId)
      throws IOException {
    // 创建 Get 对象
    Get get = new Get(Bytes.toBytes(stationId));
    // 只读取 info 列族
    get.addFamily(INFO_COLUMNFAMILY);

    // 执行查询
    Result res = table.get(get);

    // 检查结果是否为空
    if (res == null) {
      return null;
    }

    // 使用 LinkedHashMap 保持字段顺序
    Map<String, String> resultMap = new LinkedHashMap<String, String>();

    // 提取各列值
    resultMap.put("name", getValue(res, INFO_COLUMNFAMILY, NAME_QUALIFIER));
    resultMap.put("location", getValue(res, INFO_COLUMNFAMILY,
        LOCATION_QUALIFIER));
    resultMap.put("description", getValue(res, INFO_COLUMNFAMILY,
        DESCRIPTION_QUALIFIER));

    return resultMap;
  }

  /**
   * 从 Result 中提取列值,处理空值
   */
  private static String getValue(Result res, byte[] cf, byte[] qualifier) {
    byte[] value = res.getValue(cf, qualifier);
    return value == null? "": Bytes.toString(value);
  }

  /**
   * 命令行入口
   */
  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseStationQuery <station_id>");
      return -1;
    }

    // 创建配置和连接
    Configuration config = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(config);
    try {
      // 获取 Table 实例
      TableName tableName = TableName.valueOf("stations");
      Table table = connection.getTable(tableName);
      try {
        // 查询站点信息
        Map<String, String> stationInfo = getStationInfo(table, args[0]);

        // 检查站点是否存在
        if (stationInfo == null) {
          System.err.printf("Station ID %s not found.\n", args[0]);
          return -1;
        }

        // 打印站点信息
        for (Map.Entry<String, String> station : stationInfo.entrySet()) {
          System.out.printf("%s\t%s\n", station.getKey(), station.getValue());
        }
        return 0;
      } finally {
        // 关闭 Table
        table.close();
      }
    } finally {
      // 关闭 Connection
      connection.close();
    }
  }

  /**
   * main 方法
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new NewHBaseStationQuery(), args);
    System.exit(exitCode);
  }
}