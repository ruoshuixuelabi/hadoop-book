import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 查询 HBase 表中气象站点的详细信息
 *
 * 此查询器根据站点ID查询 stations 表,获取站点的:
 * - name (名称)
 * - location (位置)
 * - description (描述)
 *
 * /*[*
 * stations 表存储气象站点的元数据,行键为站点ID,
 * 使用 "info" 列族存储站点的描述性信息。
 * *]*/
public class HBaseStationQuery extends Configured implements Tool {
  // 定义 "info" 列族的字节数组表示
  static final byte[] INFO_COLUMNFAMILY = Bytes.toBytes("info");
  // 定义 "name" 列的限定符字节数组
  static final byte[] NAME_QUALIFIER = Bytes.toBytes("name");
  // 定义 "location" 列的限定符字节数组
  static final byte[] LOCATION_QUALIFIER = Bytes.toBytes("location");
  // 定义 "description" 列的限定符字节数组
  static final byte[] DESCRIPTION_QUALIFIER = Bytes.toBytes("description");

  /**
   * 获取指定站点的信息
   *
   * 使用 HTable.get() 执行单行查询,获取站点的所有元数据字段。
   *
   * @param table 已打开的 HBase 表实例
   * @param stationId 气象站点ID (作为行键)
   * @return 包含站点信息的 Map,键为字段名,值为字段值;若站点不存在则返回 null
   * @throws IOException 当发生 IO 错误时
   *
   * Get 操作流程:
   * 1. 创建 Get 对象,指定行键
   * 2. 设置要读取的列族 (可选,不设置则读取所有列族)
   * 3. 调用 table.get(get) 执行查询
   * 4. 从 Result 中提取各列的值
   */
  public Map<String, String> getStationInfo(HTable table, String stationId)
      throws IOException {
    // 创建 Get 对象,使用站点ID作为行键
    Get get = new Get(Bytes.toBytes(stationId));
    // 指定只读取 "info" 列族,减少网络传输数据量
    get.addFamily(INFO_COLUMNFAMILY);

    // 执行 Get 操作,获取结果
    Result res = table.get(get);

    // 检查站点是否存在
    if (res == null) {
      return null;
    }

    // 使用 LinkedHashMap 保持字段顺序 (name, location, description)
    Map<String, String> resultMap = new LinkedHashMap<String, String>();

    // 提取各列的值,存入 Map
    // getValue() 返回字节数组,需要转换为 String
    resultMap.put("name", getValue(res, INFO_COLUMNFAMILY, NAME_QUALIFIER));
    resultMap.put("location", getValue(res, INFO_COLUMNFAMILY,
        LOCATION_QUALIFIER));
    resultMap.put("description", getValue(res, INFO_COLUMNFAMILY,
        DESCRIPTION_QUALIFIER));

    return resultMap;
  }

  /**
   * 从 Result 中提取指定列的值,处理空值情况
   *
   * @param res Get 操作的结果对象
   * @param cf 列族字节数组
   * @param qualifier 列限定符字节数组
   * @return 列值的字符串表示,若列不存在则返回空字符串
   */
  private static String getValue(Result res, byte[] cf, byte[] qualifier) {
    // res.getValue() 返回字节数组,若列不存在则返回 null
    byte[] value = res.getValue(cf, qualifier);
    // 处理空值情况,避免 NullPointerException
    return value == null? "": Bytes.toString(value);
  }

  /**
   * 命令行入口: 查询并打印站点信息
   *
   * @param args 命令行参数, args[0] 为站点ID
   * @return 执行状态
   */
  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseStationQuery <station_id>");
      return -1;
    }

    // 创建 HTable 实例连接到 "stations" 表
    HTable table = new HTable(HBaseConfiguration.create(getConf()), "stations");
    try {
      // 获取站点信息
      Map<String, String> stationInfo = getStationInfo(table, args[0]);

      // 检查站点是否存在
      if (stationInfo == null) {
        System.err.printf("Station ID %s not found.\n", args[0]);
        return -1;
      }

      // 打印站点信息,每行格式: 字段名\t值
      for (Map.Entry<String, String> station : stationInfo.entrySet()) {
        System.out.printf("%s\t%s\n", station.getKey(), station.getValue());
      }
      return 0;
    } finally {
      // 关闭 table 连接,释放资源
      table.close();
    }
  }

  /**
   * main 方法: 使用 ToolRunner 运行查询工具
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new HBaseStationQuery(), args);
    System.exit(exitCode);
  }
}