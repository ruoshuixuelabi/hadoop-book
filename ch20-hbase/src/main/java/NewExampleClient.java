import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase 1.0 version of ExampleClient that uses {@code Connection},
 * {@code Admin} and {@code Table}.
 * 升级到 HBase 2.6.4 版本的代码
 */
public class NewExampleClient {

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        try {
            // Create table
            Admin admin = connection.getAdmin();
            try {
                TableName tableName = TableName.valueOf("test");
                //已弃用
                //As of release 2.0.0, this will be removed in HBase 3.0.0. Use TableDescriptorBuilder to build HTableDescriptor.
//                HTableDescriptor htd = new HTableDescriptor(tableName);
//                //@Deprecated // remove it in 3.0
//                HColumnDescriptor hcd = new HColumnDescriptor("data");
//                htd.addFamily(hcd);
                TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(
                                ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("data"))
                                        .setMaxVersions(3)
                                        .build())
                        .build();
                admin.createTable(htd);
                //已弃用
                //since 2.0 version and will be removed in 3.0 version. Use listTableDescriptors().
//                HTableDescriptor[] tables = admin.listTables();
//                if (tables.length != 1 &&
//                        Bytes.equals(tableName.getName(), tables[0].getTableName().getName())) {
//                    throw new IOException("Failed create of table");
//                }
                //新版本上面方法已经废弃，推荐使用如下写法
                List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
                if (tableDescriptors.size() != 1 &&
                        Bytes.equals(tableName.getName(), tableDescriptors.get(0).getTableName().getName())) {
                    throw new IOException("Failed create of table");
                }
                // Run some operations -- three puts, a get, and a scan -- against the table.
                Table table = connection.getTable(tableName);
                try {
                    for (int i = 1; i <= 3; i++) {
                        byte[] row = Bytes.toBytes("row" + i);
                        Put put = new Put(row);
                        byte[] columnFamily = Bytes.toBytes("data");
                        byte[] qualifier = Bytes.toBytes(String.valueOf(i));
                        byte[] value = Bytes.toBytes("value" + i);
                        // put.add(columnFamily, qualifier, value);
                        //新版本使用如下写法，旧版本是上面的写法，新版本上面的方法参数改变了
                        put.addColumn(columnFamily, qualifier, value);
                        table.put(put);
                    }
                    Get get = new Get(Bytes.toBytes("row1"));
                    Result result = table.get(get);
                    System.out.println("Get: " + result);
                    Scan scan = new Scan();
                    ResultScanner scanner = table.getScanner(scan);
                    try {
                        for (Result scannerResult : scanner) {
                            System.out.println("Scan: " + scannerResult);
                        }
                    }
                    finally {
                        scanner.close();
                    }
                    // Disable then drop the table
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                }
                finally {
                    table.close();
                }
            }
            finally {
                admin.close();
            }
        }
        finally {
            connection.close();
        }
    }
}