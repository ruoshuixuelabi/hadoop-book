/**
 * 文件：HBaseStationImporter.java<br>
 * 路径：src/main/java/HBaseStationImporter.java<br>
 * 描述：将气象站点元数据导入 HBase 的 "stations" 表<br>
 *<br>
 * 功能说明：<br>
 * 此导入器读取气象站点的元数据文件（包含站点ID到名称的映射），<br>
 * 并将每条记录作为一行写入 HBase 表。<br>
 *<br>
 * 数据模型：<br>
 * - 表名：stations<br>
 * - 行键：站点ID（原始 12 字符 ID）<br>
 * - 列族："info"<br>
 * - 列：<br>
 *   - info:name：站点名称<br>
 *   - info:description：站点描述（此处设为 "(unknown)"）<br>
 *   - info:location：站点位置（此处设为 "(unknown)"）<br>
 *<br>
 * 注意事项：<br>
 * stations 表存储气象站点的静态元数据，<br>
 * 与存储实时观测数据的 observations 表配合使用。<br>
 * 查询时可根据站点ID获取站点信息，再查询观测数据。<br>
 *<br>
 * 运行命令：<br>
 * hbase HBaseStationImporter <站点元数据文件路径><br>
 */
import java.io.File;
import java.io.IOException;
import java.util.Map;

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
 * 气象站点元数据导入器<br>
 * 将气象站点元数据导入 HBase 的 "stations" 表。<br>
 *<br>
 * 功能说明：<br>
 * 1. 读取气象站点的元数据文件<br>
 * 2. 解析站点ID到名称的映射关系<br>
 * 3. 为每个站点创建 Put 操作并写入 HBase<br>
 *<br>
 * 数据模型：<br>
 * - 表名：stations<br>
 * - 行键：站点ID<br>
 * - 列族：info<br>
 * - 列：name（站点名称）、description（站点描述）、location（站点位置）<br>
 */
public class HBaseStationImporter extends Configured implements Tool {

    /**
     * 执行站点数据导入<br>
     *<br>
     * 流程：<br>
     * 1. 解析命令行参数，获取元数据文件路径<br>
     * 2. 创建 Connection 和 Table 实例连接到 "stations" 表<br>
     * 3. 读取并解析站点元数据文件<br>
     * 4. 为每个站点创建 Put 操作并执行写入<br>
     * 5. 关闭 Table 和 Connection<br>
     *<br>
     * @param args 命令行参数，args[0] 为站点元数据文件路径<br>
     * @return 执行状态（0 成功，-1 失败）<br>
     * @throws IOException 如果发生 I/O 错误<br>
     */
    public int run(String[] args) throws IOException {
        // 参数验证：确保传入元数据文件路径
        if (args.length != 1) {
            // 打印用法信息到标准错误流
            System.err.println("Usage: HBaseStationImporter <input>");
            // 返回失败状态码
            return -1;
        }

        // 创建 Connection（HBase 2.x 推荐的连接管理方式）
        // Connection 是重量级对象，可共享给多个 Table 使用
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(getConf()));

        try {
            // 获取 Table 实例连接到 "stations" 表
            // Table 替代了旧的 HTable，负责与 RegionServer 通信，执行数据读写
            Table table = connection.getTable(TableName.valueOf("stations"));

            try {
                // 创建站点元数据解析器
                // NcdcStationMetadata 是自定义工具类，用于解析 NOAA 气象站点元数据
                NcdcStationMetadata metadata = new NcdcStationMetadata();

                // 初始化解析器，传入元数据文件
                // initialize() 方法会读取并解析文件内容
                metadata.initialize(new File(args[0]));

                // 获取站点ID到名称的映射
                // 返回 Map：key=站点ID，value=站点名称
                Map<String, String> stationIdToNameMap = metadata.getStationIdToNameMap();

                // 遍历每个站点，创建 Put 并写入 HBase
                for (Map.Entry<String, String> entry : stationIdToNameMap.entrySet()) {
                    // 使用站点ID作为行键
                    // 行键是 HBase 表中每行的唯一标识
                    // Put 构造函数的参数就是行键的字节数组形式
                    Put put = new Put(Bytes.toBytes(entry.getKey()));

                    // 添加站点名称列：info:name
                    // HBaseStationQuery.INFO_COLUMNFAMILY 定义了列族名 "info"
                    // HBaseStationQuery.NAME_QUALIFIER 定义了列名 "name"
                    // Bytes.toBytes() 将 Java 对象转换为字节数组
                    put.addColumn(HBaseStationQuery.INFO_COLUMNFAMILY, HBaseStationQuery.NAME_QUALIFIER,
                            Bytes.toBytes(entry.getValue()));

                    // 添加站点描述列：info:description（设为 "(unknown)"，表示未知）
                    // 部分站点可能没有描述信息，用 "(unknown)" 占位
                    put.addColumn(HBaseStationQuery.INFO_COLUMNFAMILY, HBaseStationQuery.DESCRIPTION_QUALIFIER,
                            Bytes.toBytes("(unknown)"));

                    // 添加站点位置列：info:location（设为 "(unknown)"，表示未知）
                    // 部分站点可能没有位置信息，用 "(unknown)" 占位
                    put.addColumn(HBaseStationQuery.INFO_COLUMNFAMILY, HBaseStationQuery.LOCATION_QUALIFIER,
                            Bytes.toBytes("(unknown)"));

                    // 执行写入操作
                    // 将 Put 对象发送到 HBase RegionServer
                    table.put(put);
                }

                // 返回成功状态码
                return 0;
            } finally {
                // 确保 Table 关闭，释放连接资源
                // 重要：使用完毕后必须关闭 Table，避免连接泄漏
                table.close();
            }
        } finally {
            // 关闭 Connection
            // Connection 是重量级对象，应该正确关闭
            connection.close();
        }
    }

    /**
     * 程序入口<br>
     * 使用 ToolRunner 运行导入工具。<br>
     *<br>
     * ToolRunner 是 Hadoop 提供的工具运行器，<br>
     * 负责处理命令行参数、配置加载和异常管理。<br>
     *<br>
     * @param args 命令行参数<br>
     * @throws Exception 如果执行过程中发生异常<br>
     */
    public static void main(String[] args) throws Exception {
        // 使用 ToolRunner 运行导入工具
        // ToolRunner.run() 会自动处理配置管理和异常
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new HBaseStationImporter(), args);

        // 根据返回状态码退出程序
        // 0 表示成功，非 0 表示失败
        System.exit(exitCode);
    }
}