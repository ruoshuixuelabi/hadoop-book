package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Join;

import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// Crunch version of ch09-mr-features JoinRecordWithStationName
/**
 * 用 Crunch 把 NCDC 观测记录和气象站元数据按站号连接起来
 *
 * <p>这是第 9 章 {@code JoinRecordWithStationName} 的 Crunch 版本：
 * 两份数据都变成「站号 -> X」的表，再按站号连接
 * <p>和上一节的例子不同，
 * 这里没有先聚合，
 * 而是保留每一条观测记录，
 * 连接之后每个站号下会有多条记录
 */
public class JoinRecordWithStationNameCrunch {

  /**
   * 作业入口
   *
   * @param args 三个参数：NCDC 输入目录、气象站输入目录、输出目录
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // 参数必须是三个
    if (args.length != 3) {
      System.err.println("Usage: JoinRecordWithStationNameCrunch <ncdc input> <station input> <output>");
      // 直接退出
      System.exit(-1);
    }
    // 建立管道
    Pipeline pipeline = new MRPipeline(JoinRecordWithStationNameCrunch.class);
    // NCDC 观测记录
    PCollection<String> records = pipeline.readTextFile(args[0]);
    // 气象站元数据
    PCollection<String> stations = pipeline.readTextFile(args[1]);
    // 观测记录变成「站号 -> 原始行」的表
    PTable<String, String> stationIdToRecord = records
        .parallelDo(toStationIdRecordPairsFn(), tableOf(strings(), strings()));
    // 气象站元数据变成「站号 -> 站名」的表
    PTable<String, String> stationIdToName = stations
        .parallelDo(toStationIdNamePairsFn(), tableOf(strings(), strings()));

    // 用 Crunch 内置的 join 把两个表按站号连起来
    PTable<String, Pair<String, String>> joined =
        Join.join(stationIdToRecord, stationIdToName);

    // 输出连接结果
    pipeline.writeTextFile(joined, args[2]);
    // 触发作业执行
    pipeline.run();
  }

  /**
   * 造一个把 NCDC 记录变成「站号 -> 原始行」键值对的 {@code DoFn}
   *
   * @return 供 {@code parallelDo} 使用的处理函数
   */
  private static DoFn<String, Pair<String, String>> toStationIdRecordPairsFn() {
    return new DoFn<String, Pair<String, String>>() {
      // 解析器复用
      NcdcRecordParser parser = new NcdcRecordParser();
      /**
       * 处理一行 NCDC 记录
       *
       * @param input 一行文本
       * @param emitter 用来发出结果
       */
      @Override
      public void process(String input, Emitter<Pair<String, String>> emitter) {
        // 拆出各个字段，主要是拿到站号
        parser.parse(input);
        // 值保留原始行，后面还要用
        emitter.emit(Pair.of(parser.getStationId(), input));
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
