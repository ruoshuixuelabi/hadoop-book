import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HBase 1.0+ 版本的气温数据查询器
 *
 * 升级到 HBase 2.6.4 版本的实现,相比旧版:
 * - 使用 Connection 和 Table 接口替代旧的 HTable
 * - 使用 TableDescriptor 和 ColumnFamilyDescriptor 替代旧的 HTableDescriptor 和 HColumnDescriptor
 * - Admin 接口替代旧的 HBaseAdmin
 *
 * 功能与 HBaseTemperatureQuery 相同: 查询指定站点的温度观测数据
 *
 * /*[*
 * HBase 1.0+ API 主要变化:
 * 1. Connection: 替代 HConnection,表示到集群的连接,可共享
 * 2. Table: 替代 HTable,表示到某表的连接,用完需关闭
 * 3. Admin: 替代 HBaseAdmin,用于表管理操作
 *
 * 正确使用模式:
 * Connection connection = ConnectionFactory.createConnection(config);
 * try {
 *   Table table = connection.getTable(tableName);
 *   try {
 *     // 使用 table
 *   } finally {
 *     table.close();
 *   }
 * } finally {
 *   connection.close();
 * }
 * *]*/
public class NewHBaseTemperatureQuery extends Configured implements Tool {
  // 定义 "data" 列族的字节数组
  static final byte[] DATA_COLUMNFAMILY = Bytes.toBytes("data");
  // 定义 "airtemp" 列的字节数组
  static final byte[] AIRTEMP_QUALIFIER = Bytes.toBytes("airtemp");

  /**
   * 获取指定站点的温度观测数据
   *
   * 与旧版 HBaseTemperatureQuery 逻辑相同,只是使用新版 Table API。
   * 使用 Scan 从起始行开始扫描,获取最多 maxCount 条记录。
   *
   * @param table Table 接口实例(已获取连接)
   * @param stationId 气象站点ID
   * @param maxStamp 最大时间戳(用于确定起始行键)
   * @param maxCount 最大返回记录数
   * @return 时间戳到温度值的映射 (NavigableMap)
   * @throws IOException IO 错误时抛出
   */
  public NavigableMap<Long, Integer> getStationObservations(Table table,
      String stationId, long maxStamp, int maxCount) throws IOException {
    // 生成起始行键: stationId + (Long.MAX_VALUE - maxStamp)
    byte[] startRow = RowKeyConverter.makeObservationRowKey(stationId, maxStamp);

    // TreeMap 支持按键降序遍历
    NavigableMap<Long, Integer> resultMap = new TreeMap<Long, Integer>();

    // 创建 Scan,设置起始行和要读取的列
    Scan scan = new Scan(startRow);
    scan.addColumn(DATA_COLUMNFAMILY, AIRTEMP_QUALIFIER);

    // 获取扫描器
    ResultScanner scanner = table.getScanner(scan);
    try {
      Result res;
      int count = 0;
      // 循环读取结果,直到达到最大记录数
      while ((res = scanner.next()) != null && count++ < maxCount) {
        byte[] row = res.getRow();
        byte[] value = res.getValue(DATA_COLUMNFAMILY, AIRTEMP_QUALIFIER);

        // 从行键中提取实际时间戳
        // 行键格式: stationId(12字节) + reverse_timestamp(8字节)
        // reverse_timestamp = Long.MAX_VALUE - observationTime
        // 因此: observationTime = Long.MAX_VALUE - reverse_timestamp
        Long stamp = Long.MAX_VALUE -
          Bytes.toLong(row, row.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);

        // 字节数组转整数
        Integer temp = Bytes.toInt(value);

        resultMap.put(stamp, temp);
      }
    } finally {
      // 确保扫描器关闭
      scanner.close();
    }
    return resultMap;
  }

  /**
   * 命令行入口
   *
   * @param args 命令行参数, args[0] 为站点ID
   * @return 执行状态
   */
  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseTemperatureQuery <station_id>");
      return -1;
    }

    // 创建配置和连接
    Configuration config = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(config);
    try {
      // 获取 Table 实例
      TableName tableName = TableName.valueOf("observations");
      Table table = connection.getTable(tableName);
      try {
        // 获取最多 10 条最新观测数据,降序排列
        NavigableMap<Long, Integer> observations =
            getStationObservations(table, args[0], Long.MAX_VALUE, 10).descendingMap();

        // 打印结果: 日期 时间 温度
        for (Map.Entry<Long, Integer> observation : observations.entrySet()) {
          System.out.printf("%1$tF %1$tR\t%2$s\n", observation.getKey(),
              observation.getValue());
        }
        return 0;
      } finally {
        // 关闭 Table,释放连接
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
        new NewHBaseTemperatureQuery(), args);
    System.exit(exitCode);
  }
}