import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 使用 HBase 的 {@link TableOutputFormat} 将温度数据批量加载到 HBase 表中。
 *
 * 此导入器通过 MapReduce 作业将气象数据写入 HBase,相比直接客户端写入,
 * TableOutputFormat 可以并行处理,提高导入效率。
 *
 * 工作原理:
 * - Mapper 读取气象数据文件,解析出有效记录
 * - 将每条记录转换为 HBase Put 操作
 * - TableOutputFormat 负责将 Put 写入指定的 HBase 表
 *
 * 与 {@link HBaseTemperatureDirectImporter} 的区别:
 * - DirectImporter: 每个 map 任务创建独立 HTable,直接 put,自动 flush 开启
 * - 此处使用 TableOutputFormat: 通过 shuffle 将数据发送到 reducer (此处无 reducer),
 *   由 TableOutputFormat 统一写入,减少客户端与服务端的交互次数
 *
 * 注意: 此实现仍使用 0 个 reduce 任务,数据直接从 mapper 写入 HBase
 *
 * /*[*
 *   TableOutputFormat 是 HBase 提供的 MR 输出格式,可将 MR 输出直接写入 HBase 表
 *   它内部使用 HTable 的 put 方法,但通过批量处理提高性能
 * *]*/
 */
public class HBaseTemperatureImporter extends Configured implements Tool {

  /**
   * Mapper: 解析气象数据并生成 HBase Put 操作
   *
   * 输入: (行偏移, 原始气象数据行)
   * 输出: (null, Put 对象)
   *
   * key 设为 null 是因为 TableOutputFormat 不需要 key,只关心 value (Put 对象)
   * 所有 Put 通过 shuffle 按照 partition 发送到对应的 reducer (此处无 reducer)
   * 最终由 TableOutputFormat 写入 HBase
   *
   * @param <K> 输入 key 类型,此处不使用
   */
  static class HBaseTemperatureMapper<K> extends Mapper<LongWritable, Text, K, Put> {
    // NcdcRecordParser 用于解析 NOAA 气象数据格式
    private NcdcRecordParser parser = new NcdcRecordParser();

    /**
     * 核心映射逻辑
     *
     * @param key 行偏移量 (在文件中起始位置)
     * @param value 一行原始气象数据
     * @param context Mapper 上下文,用于输出 Put 对象
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws
        IOException, InterruptedException {
      // 解析原始数据行,提取站点ID、观测时间、温度等信息
      parser.parse(value.toString());
      // 仅处理有效温度记录,过滤缺失或无效的温度数据
      if (parser.isValidTemperature()) {
        // 生成行键: 站点ID + 逆序时间戳
        // 逆序时间戳确保最新观测数据排在前面,便于快速检索最新记录
        byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(),
            parser.getObservationDate().getTime());
        // 创建 Put 对象,指定行键
        Put p = new Put(rowKey);
        // 添加列数据:
        // - 列族: "data"
        // - 列名: "airtemp" (气温)
        // - 值: 温度的整数值 (华氏度转换为原始整数值)
        p.add(HBaseTemperatureQuery.DATA_COLUMNFAMILY,
            HBaseTemperatureQuery.AIRTEMP_QUALIFIER,
            Bytes.toBytes(parser.getAirTemperature()));
        // 输出到 context,key 设为 null,TableOutputFormat 只使用 value
        context.write(null, p);
      }
    }
  }

  /**
   * 配置并执行 MapReduce 作业
   *
   * 作业配置要点:
   * 1. 设置输出格式为 TableOutputFormat,指定目标表名为 "observations"
   * 2. Mapper 处理数据,不设置 Reducer (setNumReduceTasks(0))
   * 3. 输出 key 类型为 null,因为 TableOutputFormat 不使用 key
   *
   * @param args 命令行参数, args[0] 为输入数据路径
   * @return 作业执行结果 (0 成功, 1 失败)
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: HBaseTemperatureImporter <input>");
      return -1;
    }
    // 创建 Job,使用类名作为作业名称
    Job job = new Job(getConf(), getClass().getSimpleName());
    job.setJarByClass(getClass());

    // 配置输入路径: 气象数据文件所在的 HDFS 目录
    FileInputFormat.addInputPath(job, new Path(args[0]));

    // 设置输出表名,TableOutputFormat 会将数据写入此表
    // 在 job configuration 中设置,TableOutputFormat 会读取此配置
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "observations");

    // 指定 Mapper 类
    job.setMapperClass(HBaseTemperatureMapper.class);

    // 不使用 Reducer: 数据直接从 Mapper 输出,无需聚合
    job.setNumReduceTasks(0);

    // 设置输出格式为 TableOutputFormat
    // 此格式负责将 Put 对象写入配置的 HBase 表
    job.setOutputFormatClass(TableOutputFormat.class);

    // 等待作业完成,返回执行状态
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * 程序入口点
   *
   * 使用 ToolRunner 运行工具,ToolRunner 会:
   * 1. 创建 HBase 配置
   * 2. 解析命令行参数
   * 3. 调用 run() 方法
   * 4. 返回退出码
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new HBaseTemperatureImporter(), args);
    System.exit(exitCode);
  }
}