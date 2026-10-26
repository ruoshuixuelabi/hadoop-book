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
   * HBase 1.0 版的 {@code HBaseStationQuery}
   *
   * <p>和 {@link HBaseStationQuery} 做的事情一样：
   * 按站号查出一个气象站的元数据
   * <p>差别只在 API 版本：
   * 这里用 {@code Connection} 和 {@code Table}，
   * 不再直接用 {@code HTable}
   *
 * HBase 1.0 version of HBaseStationQuery that uses {@code Connection},
 * and {@code Table}.
 */
public class NewHBaseStationQuery extends Configured implements Tool {
  // 元数据所在的列族
  static final byte[] INFO_COLUMNFAMILY = Bytes.toBytes("info");
  // 站名
  static final byte[] NAME_QUALIFIER = Bytes.toBytes("name");
  // 位置
  static final byte[] LOCATION_QUALIFIER = Bytes.toBytes("location");
  // 描述
  static final byte[] DESCRIPTION_QUALIFIER = Bytes.toBytes("description");

  /**
   * 按站号取出一个气象站的元数据
   *
   * @param table 要查询的表
   * @param stationId 站号
   * @return 「字段名 -> 字段值」的映射；站号不存在时返回 {@code null}
   * @throws IOException 访问 HBase 失败
   */
  public Map<String, String> getStationInfo(Table table, String stationId)
      throws IOException {
    // 按行键构造一次读取请求
    Get get = new Get(Bytes.toBytes(stationId));
    // 只取 info 列族
    get.addFamily(INFO_COLUMNFAMILY);
    // 执行读取
    Result res = table.get(get);
    // 没读到就说明这个站号不存在
    if (res == null) {
      // 用 null 表示「查无此站」
      return null;
    }
    // 用 LinkedHashMap 是为了保持插入顺序，输出时字段顺序是固定的
    Map<String, String> resultMap = new LinkedHashMap<String, String>();
    // 站名
    resultMap.put("name", getValue(res, INFO_COLUMNFAMILY, NAME_QUALIFIER));
    // 位置
    resultMap.put("location", getValue(res, INFO_COLUMNFAMILY,
        LOCATION_QUALIFIER));
    // 描述
    resultMap.put("description", getValue(res, INFO_COLUMNFAMILY,
        DESCRIPTION_QUALIFIER));
    // 返回查询结果
    return resultMap;
  }

  /**
   * 从一行结果里取出一个字段的值
   *
   * @param res 一行结果
   * @param cf 列族
   * @param qualifier 限定符
   * @return 字段的字符串值；字段不存在时返回空串
   */
  private static String getValue(Result res, byte[] cf, byte[] qualifier) {
    // 按「列族 + 限定符」取字节数组
    byte[] value = res.getValue(cf, qualifier);
    // 字段缺失时给一个空串，而不是 null
    // 这样调用方遍历的时候不用到处判空
    return value == null? "": Bytes.toString(value);
  }

  /**
   * 作业入口
   *
   * @param args 一个参数：站号
   * @return 0 表示成功，-1 表示参数不对或查无此站
   * @throws IOException 访问 HBase 失败
   */
  public int run(String[] args) throws IOException {
    // 必须且只需要一个站号
    if (args.length != 1) {
      System.err.println("Usage: HBaseStationQuery <station_id>");
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
      TableName tableName = TableName.valueOf("stations");
      // 打开 stations 表
      Table table = connection.getTable(tableName);
      // 后面无论成功失败都要关掉表，所以放在 try 里
      try {
        // 查这个站的信息
        Map<String, String> stationInfo = getStationInfo(table, args[0]);
        // 站号不存在
        if (stationInfo == null) {
          // 在标准错误上说明原因
          System.err.printf("Station ID %s not found.\n", args[0]);
          // 返回错误码
          return -1;
        }
        // 一个字段一行
        for (Map.Entry<String, String> station : stationInfo.entrySet()) {
          // 用制表符分隔，方便后续处理
          System.out.printf("%s\t%s\n", station.getKey(), station.getValue());
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
        new NewHBaseStationQuery(), args);
    // 用退出码告诉外面成功还是失败
    System.exit(exitCode);
  }
}