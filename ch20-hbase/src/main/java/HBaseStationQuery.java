/**
 * 文件：HBaseStationQuery.java<br>
 * 路径：src/main/java/HBaseStationQuery.java<br>
 * 描述：查询 HBase 表中气象站点的详细信息<br>
 *<br>
 * 功能说明：<br>
 * 此查询器根据站点ID查询 stations 表，获取站点的详细信息：<br>
 * - name（名称）<br>
 * - location（位置）<br>
 * - description（描述）<br>
 *<br>
 * 数据模型：<br>
 * - 表名：stations<br>
 * - 行键：站点ID<br>
 * - 列族：info<br>
 * - 列：info:name、info:location、info:description<br>
 *<br>
 * 运行命令：<br>
 * hbase HBaseStationQuery <站点ID><br>
 */
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

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
 * 气象站点信息查询器<br>
 * 查询 HBase 表中气象站点的详细信息。<br>
 */
public class HBaseStationQuery extends Configured implements Tool {

    // 定义 "info" 列族的字节数组表示
    // 列族是 HBase 中一组列的逻辑容器
    static final byte[] INFO_COLUMNFAMILY = Bytes.toBytes("info");

    // 定义 "name" 列的限定符字节数组
    static final byte[] NAME_QUALIFIER = Bytes.toBytes("name");

    // 定义 "location" 列的限定符字节数组
    static final byte[] LOCATION_QUALIFIER = Bytes.toBytes("location");

    // 定义 "description" 列的限定符字节数组
    static final byte[] DESCRIPTION_QUALIFIER = Bytes.toBytes("description");

    /**
     * 获取指定站点的信息<br>
     *<br>
     * 使用 Table.get() 执行单行查询，获取站点的所有元数据字段。<br>
     *<br>
     * Get 操作流程：<br>
     * 1. 创建 Get 对象，指定行键<br>
     * 2. 设置要读取的列族（可选，不设置则读取所有列族）<br>
     * 3. 调用 table.get(get) 执行查询<br>
     * 4. 从 Result 中提取各列的值<br>
     *<br>
     * @param table 已打开的 Table 接口实例<br>
     * @param stationId 气象站点ID（作为行键）<br>
     * @return 包含站点信息的 Map，键为字段名，值为字段值；若站点不存在则返回 null<br>
     * @throws IOException 当发生 IO 错误时<br>
     */
    public Map<String, String> getStationInfo(Table table, String stationId)
            throws IOException {
        // 创建 Get 对象，使用站点ID作为行键
        // Get 是 HBase 的点查询操作，根据行键获取单行数据
        Get get = new Get(Bytes.toBytes(stationId));

        // 指定只读取 "info" 列族，减少网络传输数据量
        // 如果不调用 addFamily，则读取所有列族的数据
        get.addFamily(INFO_COLUMNFAMILY);

        // 执行 Get 操作，获取结果
        // Result 包含查询返回的所有列及其值
        Result res = table.get(get);

        // 检查站点是否存在
        // 如果行键不存在，Result 为空
        if (res == null) {
            return null;
        }

        // 使用 LinkedHashMap 保持字段顺序（name, location, description）
        // LinkedHashMap 维护插入顺序，使得输出结果顺序固定
        Map<String, String> resultMap = new LinkedHashMap<String, String>();

        // 提取各列的值，存入 Map
        // getValue() 返回字节数组，需要转换为 String
        resultMap.put("name", getValue(res, INFO_COLUMNFAMILY, NAME_QUALIFIER));
        resultMap.put("location", getValue(res, INFO_COLUMNFAMILY,
                LOCATION_QUALIFIER));
        resultMap.put("description", getValue(res, INFO_COLUMNFAMILY,
                DESCRIPTION_QUALIFIER));

        return resultMap;
    }

    /**
     * 从 Result 中提取指定列的值，处理空值情况<br>
     *<br>
     * @param res Get 操作的结果对象<br>
     * @param cf 列族字节数组<br>
     * @param qualifier 列限定符字节数组<br>
     * @return 列值的字符串表示，若列不存在则返回空字符串<br>
     */
    private static String getValue(Result res, byte[] cf, byte[] qualifier) {
        // res.getValue() 返回字节数组，若列不存在则返回 null
        // 需要处理空值情况，避免 NullPointerException
        byte[] value = res.getValue(cf, qualifier);

        // 三元运算符：如果值为 null，返回空字符串；否则转换为 String
        return value == null ? "" : Bytes.toString(value);
    }

    /**
     * 命令行入口：查询并打印站点信息<br>
     *<br>
     * @param args 命令行参数，args[0] 为站点ID<br>
     * @return 执行状态（0 成功，-1 失败）<br>
     * @throws IOException 如果发生 I/O 错误<br>
     */
    public int run(String[] args) throws IOException {
        // 参数验证：确保传入站点ID
        if (args.length != 1) {
            System.err.println("Usage: HBaseStationQuery <station_id>");
            return -1;
        }

        // 创建 Connection（HBase 2.x 推荐的连接管理方式）
        // Connection 是重量级对象，可共享给多个 Table 使用
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(getConf()));

        try {
            // 获取 Table 实例
            // Table 是 HBase 2.x 推荐的表操作接口
            Table table = connection.getTable(TableName.valueOf("stations"));

            try {
                // 获取站点信息
                Map<String, String> stationInfo = getStationInfo(table, args[0]);

                // 检查站点是否存在
                if (stationInfo == null) {
                    // 打印错误信息到标准错误流
                    System.err.printf("Station ID %s not found.\n", args[0]);
                    return -1;
                }

                // 打印站点信息，每行格式：字段名\t值
                for (Map.Entry<String, String> station : stationInfo.entrySet()) {
                    System.out.printf("%s\t%s\n", station.getKey(), station.getValue());
                }

                return 0;
            } finally {
                // 关闭 table 连接，释放资源
                // 重要：使用完毕后必须关闭 Table
                table.close();
            }
        } finally {
            // 关闭 Connection
            // Connection 是重量级对象，应该正确关闭
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
        // ToolRunner.run() 会自动处理配置管理和异常
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new HBaseStationQuery(), args);

        // 根据返回状态码退出程序
        System.exit(exitCode);
    }
}