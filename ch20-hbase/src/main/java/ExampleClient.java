import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase 客户端操作示例
 *
 * 演示 HBase 表的创建、写入、读取、禁用和删除操作。
 *
 * 操作流程:
 * 1. 创建 HBaseAdmin 实例用于表管理
 * 2. 创建表 "test",包含一个列族 "data"
 * 3. 执行操作:
 *    - 写入 3 行 Put 操作
 *    - 读取 1 行 Get 操作
 *    - 扫描全表 Scan 操作
 * 4. 禁用并删除表
 *
 * /*[*
 * HBaseAdmin 已在 HBase 2.x 中被废弃,推荐使用 Admin 接口。
 * 参见 NewExampleClient 获取新版 API 示例。
 * *]*/
public class ExampleClient {

  /**
   * 主方法: 执行完整的 HBase 表操作流程
   *
   * @param args 命令行参数(未使用)
   */
  public static void main(String[] args) throws IOException {
    // 创建 HBase 配置 (合并 hbase-site.xml 和 hadoop 配置)
    Configuration config = HBaseConfiguration.create();

    // 创建 HBaseAdmin 用于表管理操作 (创建、删除、禁用表等)
    // HBaseAdmin 继承自 HBaseAdminUtilities,负责与 HBase Master 通信
    HBaseAdmin admin = new HBaseAdmin(config);
    try {
      // 定义表名
      TableName tableName = TableName.valueOf("test");

      // 创建表描述符
      HTableDescriptor htd = new HTableDescriptor(tableName);

      // 创建列族描述符
      // 参数: 列族名称
      // 列族是 HBase 的基本存储单元,同一列族的数据存储在一起
      HColumnDescriptor hcd = new HColumnDescriptor("data");

      // 将列族添加到表描述符
      htd.addFamily(hcd);

      // 在 HBase 中创建表
      admin.createTable(htd);

      // 验证表是否创建成功
      HTableDescriptor[] tables = admin.listTables();
      if (tables.length != 1 &&
          Bytes.equals(tableName.getName(), tables[0].getTableName().getName())) {
        throw new IOException("Failed create of table");
      }

      // /*[*
      // 执行操作: 3 个 Put, 1 个 Get, 1 个 Scan
      // *]*/

      // 创建 HTable 实例用于 CRUD 操作
      // HTable 负责与 RegionServer 通信,执行实际的数据读写
      HTable table = new HTable(config, tableName);
      try {
        // 执行 3 次 Put 操作,写入 3 行数据
        for (int i = 1; i <= 3; i++) {
          // 构建行键: "row1", "row2", "row3"
          byte[] row = Bytes.toBytes("row" + i);

          // 创建 Put 对象,指定行键
          Put put = new Put(row);

          // 定义列族和列名
          byte[] columnFamily = Bytes.toBytes("data");
          byte[] qualifier = Bytes.toBytes(String.valueOf(i));

          // 定义列值
          byte[] value = Bytes.toBytes("value" + i);

          // 添加列: 列族,列名,值
          // 每行只有一列: data:1 -> "value1", data:2 -> "value2", data:3 -> "value3"
          put.add(columnFamily, qualifier, value);

          // 执行写入
          // auto-flush 开启,每次 put 后立即发送到 RegionServer
          table.put(put);
        }

        // 执行 Get 操作: 读取 row1 的所有列
        // Get 是点查询,根据行键获取单行数据
        Get get = new Get(Bytes.toBytes("row1"));
        Result result = table.get(get);
        System.out.println("Get: " + result);

        // 执行 Scan 操作: 扫描整张表
        // Scan 是范围查询,用于遍历表中多行数据
        Scan scan = new Scan();
        // 不设置起止行意味着扫描全表
        ResultScanner scanner = table.getScanner(scan);
        try {
          // 遍历扫描结果
          for (Result scannerResult : scanner) {
            System.out.println("Scan: " + scannerResult);
          }
        } finally {
          // 关闭扫描器,释放资源
          scanner.close();
        }

        // /*[*
        // 禁用并删除表
        // 禁用表后才能删除,防止数据损坏
        // *]*/

        // 禁用表: 阻止读写操作,用于表结构修改或删除前
        admin.disableTable(tableName);

        // 删除表: 永久删除表及其所有数据
        admin.deleteTable(tableName);
      } finally {
        // 关闭 HTable,释放连接资源
        table.close();
      }
    } finally {
      // 关闭 HBaseAdmin,释放管理连接
      admin.close();
    }
  }
}