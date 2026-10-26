package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// Crunch version of ch08-mr-types MaxTemperatureWithMultipleInputsCrunch
// TODO: sanity check output
/**
 * 用 Crunch 求每年的最高气温，输入是两份不同格式的气象数据
 *
 * <p>这是第 8 章 {@code MaxTemperatureWithMultipleInputs} 的 Crunch 版本：
 * 一份是 NCDC 的定长格式记录，一份是 Met Office 的逗号分隔记录，
 * 两者都先变成「年 -> 气温」的表，
 * 再用 {@code union} 合并起来一起求最大值
 */
public class MaxTemperatureWithMultipleInputsCrunch {

  /**
   * 作业入口
   *
   * @param args 三个参数：NCDC 输入目录、Met Office 输入目录、输出目录
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // 参数必须是三个
    if (args.length != 3) {
      System.err.println("Usage: MaxTemperatureWithMultipleInputsCrunch <ncdc input> <metoffice input> <output>");
      // 直接退出
      System.exit(-1);
    }

    // 建立管道
    Pipeline pipeline = new MRPipeline(MaxTemperatureWithMultipleInputsCrunch.class);

    // NCDC 数据：定长格式，用 NcdcRecordParser 解析
    PTable<String, Integer> ncdc = pipeline.readTextFile(args[0])
        .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()));
    // Met Office 数据：逗号分隔格式，用 MetOfficeRecordParser 解析
    PTable<String, Integer> metOffice = pipeline.readTextFile(args[1])
        .parallelDo(metOfficeToYearTempPairsFn(), tableOf(strings(), ints()));

    // 两份数据合并成一张表，再按年分组求最大值
    // union 只是把两张表的记录放在一起，并不做去重
    PTable<String, Integer> maxTemps = ncdc
      .union(metOffice)
      .groupByKey()
      .combineValues(Aggregators.MAX_INTS());
    
    // 结果写成文本文件
    pipeline.writeTextFile(maxTemps, args[2]);
    // 触发作业执行
    pipeline.run();
  }

  /**
   * 造一个把 NCDC 记录变成「年 -> 气温」键值对的 {@code DoFn}
   *
   * @return 供 {@code parallelDo} 使用的处理函数
   */
  private static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
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
          // 键是年份，值是气温
          emitter.emit(Pair.of(parser.getYear(), parser.getAirTemperature()));
        }
      }
    };
  }

  /**
   * 造一个把 Met Office 记录变成「年 -> 气温」键值对的 {@code DoFn}
   *
   * <p>Met Office 的温度是带一位小数的，
   * 解析器里已经把它乘以 10 变成了整数，
   * 这样才能和 NCDC 的整数温度放在同一张表里比较
   *
   * @return 供 {@code parallelDo} 使用的处理函数
   */
  private static DoFn<String, Pair<String, Integer>> metOfficeToYearTempPairsFn() {
    return new DoFn<String, Pair<String, Integer>>() {
      // 解析器复用
      MetOfficeRecordParser parser = new MetOfficeRecordParser();
      /**
       * 处理一行 Met Office 记录
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
          // 键是年份，值是气温
          emitter.emit(Pair.of(parser.getYear(), parser.getAirTemperature()));
        }
      }
    };
  }
}
