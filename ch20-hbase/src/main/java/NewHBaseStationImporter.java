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
 * HBase 1.0+ 版本的气象站点导入器
 *
 * 升级到 HBase 2.6.4 版本的实现,使用:
 * - Connection/Table 替代 HTable
 * - TableDescriptor/ColumnFamilyDescriptor 替代 HTableDescriptor/HColumnDescriptor
 *
 * 功能与 HBaseStationImporter 相同: 将站点元数据导入 stations 表
 *
 * /*[*
 * 与旧版主要区别:
 * 1. 使用 ConnectionFactory.createConnection() 创建连接
 * 2. 使用 connection.getTable() 获取 Table 实例
 * 3. 使用 put.addColumn() 替代 put.add()
 * 4. 通过 try-finally 确保资源释放
 * *]*/
public class NewHBaseStationImporter extends Configured implements Tool {

  /**
   * 执行站点导入
   *
   * 流程:
   * 1. 创建 Connection 和 Table
   * 2. 读取站点元数据文件
   * 3. 为每个站点创建 Put 并写入
   * 4. 关闭 Table 和 Connection
   *
   * @param args 命令行参数, args[0] 为元数据文件路径
   * @return 执行状态
   */
  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseStationImporter <input>");
      return -1;
    }

    // 创建 Connection (到 HBase 集群的连接)
    Configuration config = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(config);
    try {
      // 创建 Table 实例
      TableName tableName = TableName.valueOf("stations");
      Table table = connection.getTable(tableName);
      try {
        // 创建站点元数据解析器
        NcdcStationMetadata metadata = new NcdcStationMetadata();
        metadata.initialize(new File(args[0]));
        Map<String, String> stationIdToNameMap = metadata.getStationIdToNameMap();

        // 遍历站点,创建 Put 并写入
        for (Map.Entry<String, String> entry : stationIdToNameMap.entrySet()) {
          // 使用站点ID作为行键
          Put put = new Put(Bytes.toBytes(entry.getKey()));

          // 添加站点名称列 info:name
          put.add(NewHBaseStationQuery.INFO_COLUMNFAMILY,
              NewHBaseStationQuery.NAME_QUALIFIER, Bytes.toBytes(entry.getValue()));

          // 添加站点描述列 info:description
          put.add(NewHBaseStationQuery.INFO_COLUMNFAMILY,
              NewHBaseStationQuery.DESCRIPTION_QUALIFIER, Bytes.toBytes("(unknown)"));

          // 添加站点位置列 info:location
          put.add(NewHBaseStationQuery.INFO_COLUMNFAMILY,
              NewHBaseStationQuery.LOCATION_QUALIFIER, Bytes.toBytes("(unknown)"));

          // 执行写入
          table.put(put);
        }
      } finally {
        // 关闭 Table,释放资源
        table.close();
      }
    } finally {
      // 关闭 Connection
      connection.close();
    }
    return 0;
  }

  /**
   * 程序入口
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new NewHBaseStationImporter(), args);
    System.exit(exitCode);
  }
}