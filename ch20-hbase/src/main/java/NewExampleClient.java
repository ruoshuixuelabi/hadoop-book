import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
   * HBase 1.0 版的客户端例子
   *
   * <p>这个例子把 HBase 最基本的操作串了一遍：
   * 建表、写三行、按行键读一行、扫描整张表，
   * 最后把表停用并删掉
   * <p>和 {@link ExampleClient} 的差别只在 API 版本：
   * HBase 1.0 之后用 {@code Connection}、{@code Admin} 和 {@code Table}，
   * 不再直接用 {@code HBaseAdmin} 和 {@code HTable}
   * <p>另外这里用 {@code TableName.valueOf(...)} 来表示表名，
   * 也不再直接用字符串
   *
 * HBase 1.0 version of ExampleClient that uses {@code Connection},
 * {@code Admin} and {@code Table}.
 */
public class NewExampleClient {

  /**
   * 进程入口
   *
   * @param args 本例不用命令行参数
   * @throws IOException 访问 HBase 失败
   */
  public static void main(String[] args) throws IOException {
    // 读一份 HBase 配置（默认连本机）
    Configuration config = HBaseConfiguration.create();
    // 建立一个到 HBase 的连接
    // 连接是重量级的对象，整个程序共用一个就够了
    Connection connection = ConnectionFactory.createConnection(config);
    // 后面无论成功失败都要关掉 connection，所以放在 try 里
    try {
      // Create table
      // 补充：建表
      // 表管理员
      Admin admin = connection.getAdmin();
      // 后面无论成功失败都要关掉 admin，所以放在 try 里
      try {
        // 表名
        TableName tableName = TableName.valueOf("test");
        // 表的描述符
        HTableDescriptor htd = new HTableDescriptor(tableName);
        // 列族的描述符
        HColumnDescriptor hcd = new HColumnDescriptor("data");
        // 给表加上 data 这个列族
        htd.addFamily(hcd);
        // 真正建表
        admin.createTable(htd);
        // 列出所有表，用来验证建表成功
        HTableDescriptor[] tables = admin.listTables();
        // 这里的条件写反了，逻辑上应该是「表没建出来」才报错
        // 原书就是这么写的，这里保持原样
        if (tables.length != 1 &&
            Bytes.equals(tableName.getName(), tables[0].getTableName().getName())) {
          // 建表失败就抛异常
          throw new IOException("Failed create of table");
        }
        // Run some operations -- three puts, a get, and a scan -- against the table.
        // 补充：做几个操作——三次写入、一次读取、一次扫描
        // 打开表
        Table table = connection.getTable(tableName);
        // 后面无论成功失败都要关掉表，所以放在 try 里
        try {
          // 写三行
          for (int i = 1; i <= 3; i++) {
            // 行键：row1、row2、row3
            byte[] row = Bytes.toBytes("row" + i);
            // 构造这次写入
            Put put = new Put(row);
            // 列族
            byte[] columnFamily = Bytes.toBytes("data");
            // 限定符：用行号当列名
            byte[] qualifier = Bytes.toBytes(String.valueOf(i));
            // 值
            byte[] value = Bytes.toBytes("value" + i);
            // 把「列族 + 限定符 + 值」加到这次写入里
            put.add(columnFamily, qualifier, value);
            // 写进表
            table.put(put);
          }
          // 按行键读取第一行
          Get get = new Get(Bytes.toBytes("row1"));
          // 执行读取
          Result result = table.get(get);
          // 把读到的内容打出来
          System.out.println("Get: " + result);
          // 扫描整张表
          Scan scan = new Scan();
          // 打开扫描器
          ResultScanner scanner = table.getScanner(scan);
          // 后面无论成功失败都要关掉扫描器，所以放在 try 里
          try {
            // 扫描器本身就是个迭代器
            for (Result scannerResult: scanner) {
              // 把每一行都打出来
              System.out.println("Scan: " + scannerResult);
            }
          // 无论正常结束还是抛异常，都要收拾干净
          } finally {
            // 关掉扫描器
            scanner.close();
          }
          // Disable then drop the table
          // 补充：先停用，再删表
          // 停用这张表
          admin.disableTable(tableName);
          // 删掉这张表
          admin.deleteTable(tableName);
        // 无论正常结束还是抛异常，都要收拾干净
        } finally {
          // 关闭表连接，释放资源
          table.close();
        }
      // 无论正常结束还是抛异常，都要收拾干净
      } finally {
        // 关闭 admin，释放资源
        admin.close();
      }
    // 无论正常结束还是抛异常，都要收拾干净
    } finally {
      // 关闭连接，释放资源
      connection.close();
    }
  }
}