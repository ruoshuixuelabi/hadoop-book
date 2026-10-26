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

// Crunch version of ch09-mr-features MaxTemperatureByStationNameUsingDistributedCacheFile
/**
 * 用 Crunch 求每个气象站的最高气温，并把站号换成站名
 *
 * <p>这是第 9 章 {@code MaxTemperatureByStationNameUsingDistributedCacheFile}
 * 的 Crunch 版本：
 * 数据分成两份——一份是 NCDC 观测记录，一份是气象站元数据，
 * 最后按站号把两者join 起来
 * <p>Crunch 里 join 有好几种策略，
 * 这里用的是「map 端连接」：
 * 把较小的表广播到各个 map 任务里，在 map 端完成匹配
 */
public class MaxTemperatureByStationNameCrunch {

  /**
   * 作业入口
   *
   * @param args 三个参数：NCDC 输入目录、气象站输入目录、输出目录
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // 参数必须是三个
    if (args.length != 3) {
      System.err.println("Usage: MaxTemperatureByStationNameCrunch <ncdc input> <station input> <output>");
      // 直接退出
      System.exit(-1);
    }
    // 建立管道
    Pipeline pipeline = new MRPipeline(MaxTemperatureByStationNameCrunch.class);

    // NCDC 观测记录
    PCollection<String> records = pipeline.readTextFile(args[0]);
    // 气象站元数据
    PCollection<String> stations = pipeline.readTextFile(args[1]);

    // 按站号分组，求出每个站的最高气温
    PTable<String, Integer> maxTemps = records
        .parallelDo(toStationIdTempPairsFn(), tableOf(strings(), ints()))
        .groupByKey()
        .combineValues(Aggregators.MAX_INTS());
    // 气象站元数据变成「站号 -> 站名」的表
    PTable<String, String> stationIdToName = stations
        .parallelDo(toStationIdNamePairsFn(), tableOf(strings(), strings()));

    // 选定 map 端连接策略：把右边的小表广播到各个 map 任务里
    JoinStrategy<String, Integer, String> mapsideJoin = MapsideJoinStrategy.create();
    // 两个表按站号连接起来
    // INNER_JOIN 表示只保留两边都有的站号
    PTable<String, Pair<Integer, String>> joined =
        mapsideJoin.join(maxTemps, stationIdToName, JoinType.INNER_JOIN);
    // 输出连接结果
    pipeline.writeTextFile(joined, args[2]);
    // 触发作业执行
    pipeline.run();
  }

  /**
   * 造一个把 NCDC 记录变成「站号 -> 气温」键值对的 {@code DoFn}
   *
   * @return 供 {@code parallelDo} 使用的处理函数
   */
  private static DoFn<String, Pair<String, Integer>> toStationIdTempPairsFn() {
    return new DoFn<String, Pair<String, Integer>>() {
      // 解析器复用
      NcdcRecordParser parser = new NcdcRecordParser();
      /**
       * 处理一行 NCDC 记录
       *
       * @param input 一行文本
       * @param emitter 用来发出结果
       */
      @Override
      public void process(String input, Emitter<Pair<String, Integer>> emitter) {
        // 拆成各个字段
        parser.parse(input);
        // 只要温度合格的记录
        if (parser.isValidTemperature()) {
          // 键是站号
          emitter.emit(Pair.of(parser.getStationId(), parser.getAirTemperature()));
        }
      }
    };
  }
  /**
   * 造一个把气象站元数据变成「站号 -> 站名」键值对的 {@code DoFn}
   *
   * @return 供 {@code parallelDo} 使用的处理函数
   */
  private static DoFn<String, Pair<String, String>> toStationIdNamePairsFn() {
    return new DoFn<String, Pair<String, String>>() {
      // 解析器复用
      NcdcStationMetadataParser parser = new NcdcStationMetadataParser();
      /**
       * 处理一行气象站元数据
       *
       * @param input 一行文本
       * @param emitter 用来发出结果
       */
      @Override
      public void process(String input, Emitter<Pair<String, String>> emitter) {
        // 解析成功才用；表头行之类的会被解析器拒绝
        if (parser.parse(input)) {
          // 键是站号，值是站名
          emitter.emit(Pair.of(parser.getStationId(), parser.getStationName()));
        }
      }
    };
  }

}
