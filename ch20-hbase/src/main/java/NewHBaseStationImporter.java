import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
   * HBase 1.0 版的 {@code HBaseStationImporter}
   *
   * <p>和 {@link HBaseStationImporter} 做的事情一样：
   * 把 NCDC 气象站元数据导入 stations 表
   * <p>差别只在 API 版本：
   * 这里用 {@code Connection} 和 {@code Table}，
   * 不再直接用 {@code HTable}
   *
 * HBase 1.0 version of HBaseStationImporter that uses {@code Connection},
 * and {@code Table}.
 */
public class NewHBaseStationImporter extends Configured implements Tool {
  
  /**
   * 作业入口
   *
   * @param args 一个参数：元数据文件
   * @return 0 表示成功，-1 表示参数不对
   * @throws IOException 读写 HBase 失败
   */
  public int run(String[] args) throws IOException {
    // 必须且只需要一个输入文件
    if (args.length != 1) {
      System.err.println("Usage: HBaseStationImporter <input>");
      // 参数不对，直接返回错误码
      return -1;
    }

    // 读一份 HBase 配置
    Configuration config = HBaseConfiguration.create();
    // 建立一个到 HBase 的连接
    Connection connection = ConnectionFactory.createConnection(config);
    // 后面无论成功失败都要关掉 connection，所以放在 try 里
    try {
      // Create table
      // 补充：下面要往 stations 表里写
      // 表名
      TableName tableName = TableName.valueOf("stations");
      // 打开 stations 表
      Table table = connection.getTable(tableName);
      // 后面无论成功失败都要关掉表，所以放在 try 里
      try {
        // 元数据解析器
        NcdcStationMetadata metadata = new NcdcStationMetadata();
        // 读入元数据文件
        metadata.initialize(new File(args[0]));
        // 取出「站号 -> 站名」的映射
        Map<String, String> stationIdToNameMap = metadata.getStationIdToNameMap();

        // 一个站写一行
        for (Map.Entry<String, String> entry : stationIdToNameMap.entrySet()) {
          // 行键就是站号
          Put put = new Put(Bytes.toBytes(entry.getKey()));
          // info 列族里的 name 限定符：站名
          put.add(NewHBaseStationQuery.INFO_COLUMNFAMILY,
              NewHBaseStationQuery.NAME_QUALIFIER, Bytes.toBytes(entry.getValue()));
          // info 列族里的 description 限定符：描述
          // NCDC 的元数据里没有描述，先填个占位值
          put.add(NewHBaseStationQuery.INFO_COLUMNFAMILY,
              NewHBaseStationQuery.DESCRIPTION_QUALIFIER, Bytes.toBytes("(unknown)"));
          // info 列族里的 location 限定符：位置
          // 同样没有位置信息，先填个占位值
          put.add(NewHBaseStationQuery.INFO_COLUMNFAMILY,
              NewHBaseStationQuery.LOCATION_QUALIFIER, Bytes.toBytes("(unknown)"));
          // 把这一行写进 HBase
          table.put(put);
        }
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
    // 全部写完
    return 0;
  }

  /**
   * 进程入口
   *
   * @param args 一个参数：元数据文件
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会把 HBase 的配置和命令行参数交给这个 Tool
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new NewHBaseStationImporter(), args);
    // 用退出码告诉外面成功还是失败
    System.exit(exitCode);
  }
}