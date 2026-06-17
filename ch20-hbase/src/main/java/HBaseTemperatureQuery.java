/**
 * 文件：HBaseTemperatureQuery.java<br>
 * 路径：src/main/java/HBaseTemperatureQuery.java<br>
 * 描述：查询 HBase 表中指定气象站点的观测数据（温度记录）<br>
 *<br>
 * 功能说明：<br>
 * 此查询器通过 Scan 操作扫描气象站点的时间序列数据，<br>
 * 利用逆序时间戳行键设计，支持高效的范围查询和降序遍历。<br>
 *<br>
 * 数据模型：<br>
 * - 表名：observations<br>
 * - 行键格式：<station_id_12bytes><reverse_timestamp_8bytes><br>
 * - station_id：固定12字节的站点ID<br>
 * - reverse_timestamp：Long.MAX_VALUE - 实际时间戳，实现时间逆序<br>
 * - 列族："data"<br>
 * - 列："airtemp"（气温值）<br>
 *<br>
 * 逆序时间戳设计使得：<br>
 * 1. 同一站点的新数据行键数值更大，排在前面<br>
 * 2. Scan 时从大时间戳开始，可快速获取最新数据<br>
 * 3. 避免了需要时间过滤的场景<br>
 *<br>
 * 运行命令：<br>
 * hbase HBaseTemperatureQuery <站点ID><br>
 */
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

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
 * 气象观测数据查询器<br>
 * 查询 HBase 表中指定气象站点的观测数据（温度记录）。<br>
 */
public class HBaseTemperatureQuery extends Configured implements Tool {
    // 定义 "data" 列族的字节数组表示，用于 HBase API 调用
    static final byte[] DATA_COLUMNFAMILY = Bytes.toBytes("data");

    // 定义 "airtemp" 列（气温）的字节数组表示
    static final byte[] AIRTEMP_QUALIFIER = Bytes.toBytes("airtemp");

    /**
     * 获取指定站点的最新观测数据<br>
     *<br>
     * 使用 Scan 从指定的起始行开始扫描，获取最多 maxCount 条温度记录。<br>
     * 通过逆序时间戳实现获取最新观测值（最新数据行键值最大）。<br>
     *<br>
     * @param table HBase 表实例（已打开连接）<br>
     * @param stationId 气象站点ID<br>
     * @param maxStamp 最大时间戳（用于确定起始行键）<br>
     * @param maxCount 最大返回记录数<br>
     * @return 时间戳到温度值的映射（NavigableMap，支持降序遍历）<br>
     *<br>
     * 行键生成逻辑：<br>
     * 起始行键 = stationId + (Long.MAX_VALUE - maxStamp)<br>
     * 由于逆序设计，maxStamp 为 Long.MAX_VALUE 时从最新数据开始<br>
     *<br>
     * @throws IOException 如果发生 I/O 错误<br>
     */
    public NavigableMap<Long, Integer> getStationObservations(Table table,
                                                            String stationId, long maxStamp, int maxCount) throws IOException {
        // 生成起始行键，用于定位到指定站点的数据区域
        // makeObservationRowKey 返回: stationId(12字节) + reverse_timestamp(8字节)
        byte[] startRow = RowKeyConverter.makeObservationRowKey(stationId, maxStamp);

        // 使用 TreeMap 存储结果，支持按键（时间戳）降序遍历
        NavigableMap<Long, Integer> resultMap = new TreeMap<Long, Integer>();

        // 创建 Scan 对象，设置起始行
        // Scan 是 HBase 的范围查询操作，用于遍历多行数据
        // HBase 2.x 推荐使用 withStartRow() 方法替代构造函数参数
        Scan scan = new Scan().withStartRow(startRow);

        // 只扫描 "data:airtemp" 列，减少数据传输量
        // addColumn() 指定要读取的列
        scan.addColumn(DATA_COLUMNFAMILY, AIRTEMP_QUALIFIER);

        // 获取扫描器，用于遍历结果集
        // ResultScanner 用于迭代获取 Scan 操作的结果
        ResultScanner scanner = table.getScanner(scan);

        try {
            Result res;
            int count = 0;

            // 循环读取扫描结果，直到达到最大记录数
            // scanner.next() 返回下一行数据，到达末尾时返回 null
            while ((res = scanner.next()) != null && count++ < maxCount) {
                // 获取行键字节数组
                byte[] row = res.getRow();

                // 获取气温列的值
                byte[] value = res.getValue(DATA_COLUMNFAMILY, AIRTEMP_QUALIFIER);

                // 从行键中提取实际时间戳
                // 行键末尾 8 字节存储的是 reverse_timestamp = Long.MAX_VALUE - observationTime
                // 因此: observationTime = Long.MAX_VALUE - reverse_timestamp
                Long stamp = Long.MAX_VALUE -
                        Bytes.toLong(row, row.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);

                // 将字节数组形式的温度值转换为整数
                Integer temp = Bytes.toInt(value);

                // 存入 TreeMap，时间戳为 key，温度为 value
                resultMap.put(stamp, temp);
            }
        } finally {
            // 确保扫描器关闭，释放资源
            // 重要：使用完毕后必须关闭 ResultScanner
            scanner.close();
        }

        return resultMap;
    }

    /**
     * 命令行入口：根据站点ID查询并打印温度数据<br>
     *<br>
     * @param args 命令行参数，args[0] 为站点ID<br>
     * @return 执行状态（0 成功，-1 失败）<br>
     * @throws IOException 如果发生 I/O 错误<br>
     */
    public int run(String[] args) throws IOException {
        // 参数验证：确保传入站点ID
        if (args.length != 1) {
            System.err.println("Usage: HBaseTemperatureQuery <station_id>");
            return -1;
        }

        // 创建配置和连接（HBase 1.0+ API）
        // Connection 是 HBase 2.0+ 推荐的连接管理方式
        // ConnectionFactory.createConnection() 创建到 HBase 集群的连接
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(getConf()));

        try {
            // 获取 Table 实例
            // Table 是 HBase 2.0+ 推荐的表操作接口
            Table table = connection.getTable(TableName.valueOf("observations"));

            try {
                // 获取最多 10 条最新观测数据，并转换为降序 map（最新在前）
                // Long.MAX_VALUE 作为 maxStamp 表示从最新数据开始
                // descendingMap() 将 TreeMap 转换为降序排列
                NavigableMap<Long, Integer> observations =
                        getStationObservations(table, args[0], Long.MAX_VALUE, 10).descendingMap();

                // 遍历并打印结果
                for (Map.Entry<Long, Integer> observation : observations.entrySet()) {
                    // printf 格式化输出：
                    // %1$tF: 日期（yyyy-MM-dd）
                    // %1$tR: 时间（HH:mm）
                    // %2$s: 温度值
                    System.out.printf("%1$tF %1$tR\t%2$s\n", observation.getKey(),
                            observation.getValue());
                }

                return 0;
            } finally {
                // 关闭 table 连接，释放资源
                // 重要：使用完毕后必须关闭 Table
                table.close();
            }
        } finally {
            // 关闭 Connection
            // Connection 是重量级对象，应该重用并正确关闭
            connection.close();
        }
    }

    /**
     * main 方法：使用 ToolRunner 运行查询工具<br>
     *<br>
     * @param args 命令行参数<br>
     * @throws Exception 如果执行过程中发生异常<br>
     */
    public static void main(String[] args) throws Exception {
        // 使用 ToolRunner 运行查询工具
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new HBaseTemperatureQuery(), args);

        System.exit(exitCode);
    }
}
