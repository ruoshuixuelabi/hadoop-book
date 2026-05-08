/**
 * 文件：ExampleClient.java<br>
 * 路径：src/main/java/ExampleClient.java<br>
 * 描述：HBase 2.x 客户端操作示例<br>
 *<br>
 * 功能说明：<br>
 * 演示 HBase 表的创建、写入、读取、禁用和删除操作。<br>
 * 升级到 HBase 2.6.4 版本，使用新版 API：<br>
 * - Connection 替代 HConnection<br>
 * - Admin 替代 HBaseAdmin<br>
 * - Table 替代 HTable<br>
 * - TableDescriptor 替代 HTableDescriptor<br>
 * - ColumnFamilyDescriptor 替代 HColumnDescriptor<br>
 *<br>
 * 操作流程：<br>
 * 1. 创建 Connection 实例用于连接管理<br>
 * 2. 通过 connection.getAdmin() 获取 Admin 用于表管理<br>
 * 3. 创建表 "test"，包含一个列族 "data"<br>
 * 4. 执行操作：<br>
 *    - 写入 3 行 Put 操作<br>
 *    - 读取 1 行 Get 操作<br>
 *    - 扫描全表 Scan 操作<br>
 * 5. 禁用并删除表<br>
 *<br>
 * 运行命令：<br>
 * hbase ExampleClient<br>
 */
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase 2.x 客户端操作示例类<br>
 * 演示 HBase 表的创建、写入、读取、禁用和删除操作。<br>
 */
public class ExampleClient {

    /**
     * 主程序入口<br>
     * 执行完整的 HBase 表操作流程<br>
     *<br>
     * @param args 命令行参数（未使用）<br>
     * @throws IOException 如果发生 I/O 错误<br>
     */
    public static void main(String[] args) throws IOException {
        // 创建 HBase 配置（合并 hbase-site.xml 和 hadoop 配置）
        // HBaseConfiguration.create() 会自动加载 classpath 中的 hbase-default.xml
        // 并合并 hbase-site.xml 中的用户配置
        Configuration config = HBaseConfiguration.create();

        // 创建 Connection（到 HBase 集群的连接）
        // Connection 是重量级对象，可共享给多个 Table 使用
        // 一个应用通常只需要一个 Connection 实例
        Connection connection = ConnectionFactory.createConnection(config);

        try {
            // 获取 Admin 实例，用于表管理（创建、删除、禁用表等）
            // Admin 替代了废弃的 HBaseAdmin
            Admin admin = connection.getAdmin();

            try {
                // 定义表名
                // TableName.valueOf() 将字符串转换为 HBase 的 TableName 对象
                TableName tableName = TableName.valueOf("test");

                // 使用 TableDescriptorBuilder 构建表描述符
                // TableDescriptor 包含表的元数据（表名、列族列表等）
                TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
                        // 设置列族，使用 ColumnFamilyDescriptorBuilder
                        .setColumnFamily(
                                ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("data"))
                                        // 设置最大版本数为 3（保留最近 3 个版本）
                                        .setMaxVersions(3)
                                        .build())
                        .build();

                // 在 HBase 中创建表
                // admin.createTable() 会向 HBase Master 发送创建表请求
                // Master 会协调 RegionServer 创建对应的 Region
                admin.createTable(htd);

                // 验证表是否创建成功
                // listTableDescriptors() 返回当前所有表的描述符
                List<TableDescriptor> tables = admin.listTableDescriptors();

                // 检查表是否创建成功
                // 条件：表数量为 1 且表名匹配
                if (tables.size() != 1 &&
                        Bytes.equals(tableName.getName(), tables.get(0).getTableName().getName())) {
                    // 如果表创建失败，抛出 IOException
                    throw new IOException("Failed create of table");
                }

                // 执行操作：3 个 Put，1 个 Get，1 个 Scan

                // 获取 Table 实例用于 CRUD 操作
                // Table 替代了旧的 HTable，负责与 RegionServer 通信，执行实际的数据读写
                Table table = connection.getTable(tableName);

                try {
                    // 执行 3 次 Put 操作，写入 3 行数据
                    for (int i = 1; i <= 3; i++) {
                        // 构建行键：row1, row2, row3
                        // 行键是字节数组，用于唯一标识表中每一行
                        // Bytes.toBytes() 将 String 转换为字节数组
                        byte[] row = Bytes.toBytes("row" + i);

                        // 创建 Put 对象，指定行键
                        // Put 用于表示一次写入操作
                        Put put = new Put(row);

                        // 定义列族和列名
                        // 列族名称
                        byte[] columnFamily = Bytes.toBytes("data");
                        // 列名（限定符），用于区分同一列族中的不同列
                        byte[] qualifier = Bytes.toBytes(String.valueOf(i));

                        // 定义列值
                        byte[] value = Bytes.toBytes("value" + i);

                        // 添加列：列族，列名，值
                        // HBase 2.x 使用 addColumn() 替代旧版的 add()
                        // 每行只有一列：data:1 -> "value1", data:2 -> "value2", data:3 -> "value3"
                        // HBase 使用 (行键, 列族:列名, 时间戳) 作为单元格的唯一标识
                        put.addColumn(columnFamily, qualifier, value);

                        // 执行写入操作
                        // auto-flush 开启（默认），每次 put 后立即发送到 RegionServer
                        // 也可以通过 table.setAutoFlush(false) 批量提交
                        table.put(put);
                    }

                    // 执行 Get 操作：读取 row1 的所有列
                    // Get 是点查询，根据行键获取单行数据
                    // 创建 Get 对象，指定要获取的行键
                    Get get = new Get(Bytes.toBytes("row1"));

                    // 执行 Get 查询，获取结果
                    // Result 包含查询返回的所有列
                    Result result = table.get(get);

                    // 打印 Get 操作结果
                    System.out.println("Get: " + result);

                    // 执行 Scan 操作：扫描整张表
                    // Scan 是范围查询，用于遍历表中多行数据
                    // 创建 Scan 对象（不设置起止行意味着扫描全表）
                    Scan scan = new Scan();

                    // 获取扫描器
                    // ResultScanner 用于迭代获取扫描结果
                    // 内部会与 RegionServer 通信获取数据
                    ResultScanner scanner = table.getScanner(scan);

                    try {
                        // 遍历扫描结果
                        // for 循环每次迭代从 ResultScanner 获取一行数据
                        for (Result scannerResult : scanner) {
                            // 打印每行扫描结果
                            System.out.println("Scan: " + scannerResult);
                        }
                    } finally {
                        // 关闭扫描器，释放资源
                        // 重要：使用完毕后必须关闭，否则会占用连接资源
                        scanner.close();
                    }

                    // 禁用并删除表

                    // 禁用表：阻止读写操作，用于表结构修改或删除前
                    // 禁用后，表的数据仍然存在，但无法进行读写
                    admin.disableTable(tableName);

                    // 删除表：永久删除表及其所有数据
                    // 删除前必须先禁用表
                    admin.deleteTable(tableName);
                } finally {
                    // 关闭 Table，释放连接资源
                    // 重要：使用完毕后必须关闭 Table
                    table.close();
                }
            } finally {
                // 关闭 Admin，释放管理连接
                // 重要：使用完毕后必须关闭 Admin 连接
                admin.close();
            }
        } finally {
            // 关闭 Connection，释放所有资源
            // Connection 是重量级对象，应该正确关闭
            connection.close();
        }
    }
}