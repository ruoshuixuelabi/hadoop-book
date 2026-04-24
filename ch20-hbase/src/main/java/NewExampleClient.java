import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase 2.x 版本的客户端操作示例
 *
 * 升级到 HBase 2.6.4 版本的代码,使用新版 API:
 * - Connection 替代 HConnection
 * - Admin 替代 HBaseAdmin
 * - Table 替代 HTable
 * - TableDescriptor 替代 HTableDescriptor
 * - ColumnFamilyDescriptor 替代 HColumnDescriptor
 *
 * /*[*
 * 主要 API 变化:
 * 1. ConnectionFactory.createConnection() 创建连接
 * 2. connection.getAdmin() 获取 Admin 用于表管理
 * 3. connection.getTable() 获取 Table 用于数据操作
 * 4. TableDescriptorBuilder.newBuilder() 创建表描述符
 * 5. ColumnFamilyDescriptorBuilder.newBuilder() 创建列族描述符
 * 6. put.addColumn() 替代 put.add()
 * *]*/
public class NewExampleClient {

  /**
   * 主方法: 执行 HBase 表操作流程 (与 ExampleClient 相同但使用新版 API)
   */
  public static void main(String[] args) throws IOException {
    // 创建配置
    Configuration config = HBaseConfiguration.create();

    // 创建 Connection (到 HBase 集群的连接)
    // Connection 是重量级对象,可共享给多个 Table 使用
    Connection connection = ConnectionFactory.createConnection(config);
    try {
      // 获取 Admin 实例,用于表管理 (创建、删除、禁用表等)
      // Admin 替代了废弃的 HBaseAdmin
      Admin admin = connection.getAdmin();
      try {
        // 定义表名
        TableName tableName = TableName.valueOf("test");

        // 使用 TableDescriptorBuilder 构建表描述符 (替代旧的 HTableDescriptor)
        // 新 API: fluent 风格,支持链式调用
        TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
                // 设置列族,使用 ColumnFamilyDescriptorBuilder
                .setColumnFamily(
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("data"))
                                // 设置最大版本数为 3 (保留最近 3 个版本)
                                .setMaxVersions(3)
                                .build())
                .build();

        // 创建表
        admin.createTable(htd);

        // 验证表创建成功
        // listTableDescriptors() 替代废弃的 listTables()
        List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
        if (tableDescriptors.size() != 1 &&
                Bytes.equals(tableName.getName(), tableDescriptors.get(0).getTableName().getName())) {
          throw new IOException("Failed create of table");
        }

        /*[*
         * 执行操作: 3 个 Put, 1 个 Get, 1 个 Scan
         * 与 ExampleClient 相同,但使用新版 Table API
         *]*/

        // 获取 Table 实例 (用于 CRUD 操作)
        // 替代旧的 HTable
        Table table = connection.getTable(tableName);
        try {
          // 执行 3 次 Put 操作
          for (int i = 1; i <= 3; i++) {
            byte[] row = Bytes.toBytes("row" + i);
            Put put = new Put(row);
            byte[] columnFamily = Bytes.toBytes("data");
            byte[] qualifier = Bytes.toBytes(String.valueOf(i));
            byte[] value = Bytes.toBytes("value" + i);

            // 新版 API 使用 addColumn() 替代旧版的 add()
            // addColumn(columnFamily, qualifier, value)
            put.addColumn(columnFamily, qualifier, value);
            table.put(put);
          }

          // 执行 Get 操作
          Get get = new Get(Bytes.toBytes("row1"));
          Result result = table.get(get);
          System.out.println("Get: " + result);

          // 执行 Scan 操作
          Scan scan = new Scan();
          ResultScanner scanner = table.getScanner(scan);
          try {
            for (Result scannerResult : scanner) {
              System.out.println("Scan: " + scannerResult);
            }
          }
          finally {
            // 关闭扫描器
            scanner.close();
          }

          // 禁用并删除表
          admin.disableTable(tableName);
          admin.deleteTable(tableName);
        }
        finally {
          // 关闭 Table,释放资源
          table.close();
        }
      }
      finally {
        // 关闭 Admin
        admin.close();
      }
    }
    finally {
      // 关闭 Connection,释放所有资源
      connection.close();
    }
  }
}