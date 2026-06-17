package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.join.JoinStrategy;
import org.apache.crunch.lib.join.JoinType;
import org.apache.crunch.lib.join.MapsideJoinStrategy;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

/**
 * Crunch版本的MaxTemperatureByStationNameUsingDistributedCacheFile程序.
 *
 * <p>本程序演示了如何使用Crunch框架实现类似于ch09-mr-features中基于分布式缓存的
 * 按站点名称查找最高温度的功能.与原版使用分布式缓存不同,本版本使用Map端连接(Mapside Join)
 * 来关联气象站ID与站点名称,从而在Map阶段即可完成站点名称的转换.
 *
 * <p>程序处理流程:
 * <ol>
 *   <li>读取气象记录数据和气象站元数据</li>
 *   <li>从气象记录中提取站点ID和温度,计算每个站点的最高温度</li>
 *   <li>从气象站元数据中提取站点ID和名称的映射</li>
 *   <li>使用Map端连接将最高温度数据与站点名称关联</li>
 *   <li>输出最终结果</li>
 * </ol>
 *
 * <p>使用方法:
 * <pre>
 * MaxTemperatureByStationNameCrunch &lt;ncdc input&gt; &lt;station input&gt; &lt;output&gt;
 * </pre>
 *
 * <p>输入参数说明:
 * <ul>
 *   <li>ncdc input: NCDC气象数据文件路径(包含站点ID和温度记录)</li>
 *   <li>station input: 气象站元数据文件路径(包含站点ID和名称映射)</li>
 *   <li>output: 输出结果文件路径</li>
 * </ul>
 *
 * // cc Map端连接演示 - 与分布式缓存方式对比,了解两种数据关联方式
 * // vv Crunch的MapsideJoinStrategy - 广播小表到所有Map节点
 *
 * @see org.apache.crunch.impl.mr.MRPipeline
 * @see org.apache.crunch.lib.join.MapsideJoinStrategy
 */
public class MaxTemperatureByStationNameCrunch {

  /**
   * 程序入口方法.
   *
   * <p>初始化Crunch管道,读取输入数据,执行Map端连接计算每个气象站的最高温度,
   * 并将结果写入指定输出路径.
   *
   * @param args 命令行参数,包含三个必要参数:
   *              args[0] - NCDC气象数据输入路径
   *              args[1] - 气象站元数据输入路径
   *              args[2] - 输出结果路径
   * @throws Exception 如果管道执行过程中发生错误
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: MaxTemperatureByStationNameCrunch <ncdc input> <station input> <output>");
      System.exit(-1);
    }
    // 创建基于MapReduce运行的Crunch管道
    Pipeline pipeline = new MRPipeline(MaxTemperatureByStationNameCrunch.class);

    // 读取气象记录和气象站元数据两个输入源
    PCollection<String> records = pipeline.readTextFile(args[0]);
    PCollection<String> stations = pipeline.readTextFile(args[1]);

    /*[*
     * 对气象记录进行转换:
     * 1. parallelDo将每条气象记录转换为(站点ID, 温度)对
     * 2. groupByKey将相同站点的记录聚合在一起
     * 3. combineValues使用MAX_INTS聚合器计算每个站点的最高温度
     *]*/
    PTable<String, Integer> maxTemps = records
        .parallelDo(toStationIdTempPairsFn(), tableOf(strings(), ints()))
        .groupByKey()
        .combineValues(Aggregators.MAX_INTS());

    /*[*
     * 对气象站元数据进行转换:
     * parallelDo将每条元数据记录转换为(站点ID, 站点名称)对
     *]*/
    PTable<String, String> stationIdToName = stations
        .parallelDo(toStationIdNamePairsFn(), tableOf(strings(), strings()));

    // 使用Map端连接策略,将温度数据与站点名称进行关联
    // MapsideJoinStrategy会将小表(站点元数据)广播到所有Map节点
    JoinStrategy<String, Integer, String> mapsideJoin = MapsideJoinStrategy.create();
    // INNER_JOIN确保只保留两边都有的站点数据
    PTable<String, Pair<Integer, String>> joined =
        mapsideJoin.join(maxTemps, stationIdToName, JoinType.INNER_JOIN);

    // 写入输出文件并执行管道
    pipeline.writeTextFile(joined, args[2]);
    pipeline.run();
  }

  /**
   * 创建将气象记录转换为(站点ID, 温度)对的DoFn.
   *
   * <p>此DoFn负责:
   * <ul>
   *   <li>解析NCDC气象数据记录</li>
   *   <li>过滤无效的温度数据</li>
   *   <li>提取站点ID和空气温度组成键值对</li>
   * </ul>
   *
   * @return 配置好的DoFn实例,用于parallelDo转换
   */
  private static DoFn<String, Pair<String, Integer>> toStationIdTempPairsFn() {
    return new DoFn<String, Pair<String, Integer>>() {
      // 每个DoFn实例维护自己的解析器实例,保证线程安全
      NcdcRecordParser parser = new NcdcRecordParser();

      /**
       * 处理单条气象记录,提取站点ID和温度信息.
       *
       * @param input 原始气象记录字符串
       * @param emitter 用于发射转换后的键值对
       */
      @Override
      public void process(String input, Emitter<Pair<String, Integer>> emitter) {
        parser.parse(input);
        // 只处理有效温度的记录,跳过缺失或异常的的温度数据
        if (parser.isValidTemperature()) {
          emitter.emit(Pair.of(parser.getStationId(), parser.getAirTemperature()));
        }
      }
    };
  }

  /**
   * 创建将气象站元数据转换为(站点ID, 站点名称)对的DoFn.
   *
   * <p>此DoFn负责从气象站元数据中提取站点标识和对应的名称,
   * 用于后续与温度数据进行关联.
   *
   * @return 配置好的DoFn实例,用于parallelDo转换
   */
  private static DoFn<String, Pair<String, String>> toStationIdNamePairsFn() {
    return new DoFn<String, Pair<String, String>>() {
      // 维护解析器实例用于处理元数据记录
      NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

      /**
       * 处理单条气象站元数据记录.
       *
       * @param input 原始气象站元数据字符串
       * @param emitter 用于发射转换后的键值对
       */
      @Override
      public void process(String input, Emitter<Pair<String, String>> emitter) {
        // parse方法返回false表示解析失败或记录无效,跳过此类记录
        if (parser.parse(input)) {
          emitter.emit(Pair.of(parser.getStationId(), parser.getStationName()));
        }
      }
    };
  }

}
