import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 把 NCDC 气象站元数据导入 HBase 的 stations 表
 *
 * <p>行键用站号，列是 info 列族里的 name、description、location 三个限定符
 * <p>description 和 location 这两个字段在 NCDC 的元数据文件里并没有，
 * 这里先填成 {@code (unknown)}，
 * 算是把「导入程序要面对的数据不完整」这件事显式地写出来
 */
public class HBaseStationImporter extends Configured implements Tool {
  
  /**
   * 作业入口
   *
   * @param args 一个参数：元数据文件
   * @return 0 表示成功，-1 表示参数不对
   * @throws IOException 读写 HBase 失败
   */
  public int run(String[] args) throws IOException {
    // 必须且只需要一个输入文件
    if (args.length != 1) {
      System.err.println("Usage: HBaseStationImporter <input>");
      // 参数不对，直接返回错误码
      return -1;
    }
    
    // 打开 stations 表
    HTable table = new HTable(HBaseConfiguration.create(getConf()), "stations");
    // 后面无论成功失败都要关掉表，所以放在 try 里
    try {
      // 元数据解析器
      NcdcStationMetadata metadata = new NcdcStationMetadata();
      // 读入元数据文件
      metadata.initialize(new File(args[0]));
      // 取出「站号 -> 站名」的映射
      Map<String, String> stationIdToNameMap = metadata.getStationIdToNameMap();

      // 一个站写一行
      for (Map.Entry<String, String> entry : stationIdToNameMap.entrySet()) {
        // 行键就是站号
        Put put = new Put(Bytes.toBytes(entry.getKey()));
        // info 列族里的 name 限定符：站名
        put.add(HBaseStationQuery.INFO_COLUMNFAMILY, HBaseStationQuery.NAME_QUALIFIER,
            Bytes.toBytes(entry.getValue()));
        // info 列族里的 description 限定符：描述
        put.add(HBaseStationQuery.INFO_COLUMNFAMILY, HBaseStationQuery.DESCRIPTION_QUALIFIER,

            // NCDC 的元数据里没有描述，先填个占位值
            Bytes.toBytes("(unknown)"));
        // info 列族里的 location 限定符：位置
        put.add(HBaseStationQuery.INFO_COLUMNFAMILY, HBaseStationQuery.LOCATION_QUALIFIER,
            // 同样没有位置信息，先填个占位值
            Bytes.toBytes("(unknown)"));
        // 把这一行写进 HBase
        table.put(put);
      }
      // 全部写完
      return 0;
    // 无论正常结束还是抛异常，都要收拾干净
    } finally {
      // 关闭表连接，释放资源
      table.close();
    }
  }

  /**
   * 进程入口
   *
   * @param args 一个参数：元数据文件
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会把 HBase 的配置和命令行参数交给这个 Tool
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new HBaseStationImporter(), args);
    // 用退出码告诉外面成功还是失败
    System.exit(exitCode);
  }
}