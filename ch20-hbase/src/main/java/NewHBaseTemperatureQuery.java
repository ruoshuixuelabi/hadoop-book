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
   * HBase 1.0 版的 {@code HBaseTemperatureQuery}
   *
   * <p>和 {@link HBaseTemperatureQuery} 做的事情一样：
   * 查出一个气象站最近的若干条观测
   * <p>差别只在 API 版本：
   * 这里用 {@code Connection} 和 {@code Table}，
   * 不再直接用 {@code HTable}
   *
 * HBase 1.0 version of HBaseTemperatureQuery that uses {@code Connection},
 * and {@code Table}.
 */
public class NewHBaseTemperatureQuery extends Configured implements Tool {
  // 观测数据所在的列族
  static final byte[] DATA_COLUMNFAMILY = Bytes.toBytes("data");
  // 气温
  static final byte[] AIRTEMP_QUALIFIER = Bytes.toBytes("airtemp");
  
  /**
   * 取出一个气象站最近的若干条观测
   *
   * @param table 要查询的表
   * @param stationId 站号
   * @param maxStamp 只取这个时间戳之前的观测
   * @param maxCount 最多取多少条
   * @return 「观测时间 -> 气温」的映射，按时间升序
   * @throws IOException 访问 HBase 失败
   */
  public NavigableMap<Long, Integer> getStationObservations(Table table,
      String stationId, long maxStamp, int maxCount) throws IOException {
    // 扫描的起始行键
    // 因为时间戳是反序的，从这里开始往后扫就是从新到旧
    byte[] startRow = RowKeyConverter.makeObservationRowKey(stationId, maxStamp);
    // 用 TreeMap 是因为它天生按时间排好序
    NavigableMap<Long, Integer> resultMap = new TreeMap<Long, Integer>();
    // 从起始行键开始扫
    Scan scan = new Scan(startRow);
    // 只取 data 列族里的 airtemp 限定符
    scan.addColumn(DATA_COLUMNFAMILY, AIRTEMP_QUALIFIER);
    // 打开扫描器
    ResultScanner scanner = table.getScanner(scan);
    // 后面无论成功失败都要关掉扫描器，所以放在 try 里
    try {
      // 当前读到的一行
      Result res;
      // 已经读了多少条
      int count = 0;
      // 边读边数，读够 maxCount 条就停
      while ((res = scanner.next()) != null && count++ < maxCount) {
        // 行键：前面是站号，后面是反序时间戳
        byte[] row = res.getRow();
        // 气温的值
        byte[] value = res.getValue(DATA_COLUMNFAMILY, AIRTEMP_QUALIFIER);
        // 把行键末尾那 8 字节还原成原来的时间戳
        // 写进去的时候取过反，这里再取一次反
        Long stamp = Long.MAX_VALUE -
          Bytes.toLong(row, row.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
        // 气温是 4 个字节的整数
        Integer temp = Bytes.toInt(value);
        // 记下「观测时间 -> 气温」
        resultMap.put(stamp, temp);
      }
    // 无论正常结束还是抛异常，都要收拾干净
    } finally {
      // 关掉扫描器
      scanner.close();
    }
    // 返回观测结果
    return resultMap;
  }

  /**
   * 作业入口
   *
   * @param args 一个参数：站号
   * @return 0 表示成功，-1 表示参数不对
   * @throws IOException 访问 HBase 失败
   */
  public int run(String[] args) throws IOException {
    // 必须且只需要一个站号
    if (args.length != 1) {
      System.err.println("Usage: HBaseTemperatureQuery <station_id>");
      // 参数不对，直接返回错误码
      return -1;
    }

    // 读一份 HBase 配置
    Configuration config = HBaseConfiguration.create();
    // 建立一个到 HBase 的连接
    Connection connection = ConnectionFactory.createConnection(config);
    // 后面无论成功失败都要关掉 connection，所以放在 try 里
    try {
      // 表名
      TableName tableName = TableName.valueOf("observations");
      // 打开 observations 表
      Table table = connection.getTable(tableName);
      // 后面无论成功失败都要关掉表，所以放在 try 里
      try {
        // 取最近 10 条，再翻成倒序
        // descendingMap() 只换遍历方向，不复制数据
        NavigableMap<Long, Integer> observations =
            getStationObservations(table, args[0], Long.MAX_VALUE, 10).descendingMap();
        // 一条观测一行
        for (Map.Entry<Long, Integer> observation : observations.entrySet()) {
          // Print the date, time, and temperature
          // 补充：打印日期、时间和气温
          // %1$tF 是日期、%1$tR 是时间、%2$s 是气温
          System.out.printf("%1$tF %1$tR\t%2$s\n", observation.getKey(),
              observation.getValue());
        }
        // 查询成功
        return 0;
      // 无论正常结束还是抛异常，都要收拾干净
      } finally {
        // 关闭表连接，释放资源
        table.close();
      }
    // 无论正常结束还是抛异常，都要收拾干净
    } finally {
      // 关闭连接，释放资源
      connection.close();
    }
  }

  /**
   * 进程入口
   *
   * @param args 一个参数：站号
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会把 HBase 的配置和命令行参数交给这个 Tool
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new NewHBaseTemperatureQuery(), args);
    // 用退出码告诉外面成功还是失败
    System.exit(exitCode);
  }
}
