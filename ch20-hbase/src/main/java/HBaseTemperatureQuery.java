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
 * 查询 HBase 表中指定气象站点的观测数据(温度记录)
 * <p>
 * 此查询器通过 Scan 操作扫描气象站点的时间序列数据,
 * 利用逆序时间戳行键设计,支持高效的范围查询和降序遍历。
 * <p>
 * 数据模型:
 * - 表名: observations
 * - 行键格式: <station_id_12bytes><reverse_timestamp_8bytes>
 * - station_id: 固定12字节的站点ID
 * - reverse_timestamp: Long.MAX_VALUE - 实际时间戳,实现时间逆序
 * - 列族: "data"
 * - 列: "airtemp" (气温值)
 * <p>
 * 逆序时间戳设计使得:
 * 1. 同一站点的新数据行键数值更大,排在前面
 * 2. Scan 时从大时间戳开始,可快速获取最新数据
 * 3. 避免了需要时间过滤的场景
 */

public class HBaseTemperatureQuery extends Configured implements Tool {
    // 定义 "data" 列族的字节数组表示,用于 HBase API 调用
    static final byte[] DATA_COLUMNFAMILY = Bytes.toBytes("data");
    // 定义 "airtemp" 列(气温)的字节数组表示
    static final byte[] AIRTEMP_QUALIFIER = Bytes.toBytes("airtemp");

    /**
     * 获取指定站点的最新观测数据
     *
     * 使用 Scan 从指定的起始行开始扫描,获取最多 maxCount 条温度记录。
     * 通过逆序时间戳实现获取最新观测值(最新数据行键值最大)。
     *
     * @param table HBase 表实例(已打开连接)
     * @param stationId 气象站点ID
     * @param maxStamp 最大时间戳(用于确定起始行键)
     * @param maxCount 最大返回记录数
     * @return 时间戳到温度值的映射 (NavigableMap,支持降序遍历)
     *
     * 行键生成逻辑:
     * 起始行键 = stationId + (Long.MAX_VALUE - maxStamp)
     * 由于逆序设计,maxStamp 为 Long.MAX_VALUE 时从最新数据开始
     */
    public NavigableMap<Long, Integer> getStationObservations(Table table,
                                                              String stationId, long maxStamp, int maxCount) throws IOException {
        // 生成起始行键,用于定位到指定站点的数据区域
        // makeObservationRowKey 返回: stationId(12字节) + reverse_timestamp(8字节)
        byte[] startRow = RowKeyConverter.makeObservationRowKey(stationId, maxStamp);

        // 使用 TreeMap 存储结果,支持按键(时间戳)降序遍历
        NavigableMap<Long, Integer> resultMap = new TreeMap<Long, Integer>();

        // 创建 Scan 对象,设置起始行
        Scan scan = new Scan(startRow);
        // 只扫描 "data:airtemp" 列,减少数据传输量
        scan.addColumn(DATA_COLUMNFAMILY, AIRTEMP_QUALIFIER);

        // 获取扫描器,用于遍历结果集
        ResultScanner scanner = table.getScanner(scan);
        try {
            Result res;
            int count = 0;
            // 循环读取扫描结果,直到达到最大记录数
            while ((res = scanner.next()) != null && count++ < maxCount) {
                byte[] row = res.getRow();
                byte[] value = res.getValue(DATA_COLUMNFAMILY, AIRTEMP_QUALIFIER);

                // 从行键中提取实际时间戳
                // 行键末尾 8 字节存储的是 reverse_timestamp = Long.MAX_VALUE - observationTime
                // 因此: observationTime = Long.MAX_VALUE - reverse_timestamp
                Long stamp = Long.MAX_VALUE -
                        Bytes.toLong(row, row.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);

                // 将字节数组形式的温度值转换为整数
                Integer temp = Bytes.toInt(value);

                // 存入 TreeMap,时间戳为 key,温度为 value
                resultMap.put(stamp, temp);
            }
        }
        finally {
            // 确保扫描器关闭,释放资源
            scanner.close();
        }
        return resultMap;
    }

    /**
     * 命令行入口: 根据站点ID查询并打印温度数据
     *
     * 使用方式: HBaseTemperatureQuery <station_id>
     *
     * @param args 命令行参数, args[0] 为站点ID
     * @return 执行状态 (0 成功, -1 失败)
     */
    public int run(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: HBaseTemperatureQuery <station_id>");
            return -1;
        }

        // 创建配置和连接 (HBase 1.0+ API)
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(getConf()));
        try {
            // 获取 Table 实例
            Table table = connection.getTable(TableName.valueOf("observations"));
            try {
                // 获取最多 10 条最新观测数据,并转换为降序 map (最新在前)
                NavigableMap<Long, Integer> observations =
                        getStationObservations(table, args[0], Long.MAX_VALUE, 10).descendingMap();

                // 遍历并打印结果
                for (Map.Entry<Long, Integer> observation : observations.entrySet()) {
                    // %1$tF: 日期 (yyyy-MM-dd), %1$tR: 时间 (HH:mm)
                    System.out.printf("%1$tF %1$tR\t%2$s\n", observation.getKey(),
                            observation.getValue());
                }
                return 0;
            } finally {
                // 关闭 table 连接,释放资源
                table.close();
            }
        } finally {
            // 关闭 Connection
            connection.close();
        }
    }

    /**
     * main 方法: 使用 ToolRunner 运行查询工具
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new HBaseTemperatureQuery(), args);
        System.exit(exitCode);
    }
}