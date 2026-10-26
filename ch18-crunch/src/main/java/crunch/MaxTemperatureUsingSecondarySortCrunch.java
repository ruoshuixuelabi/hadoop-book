package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Sort;

import static org.apache.crunch.lib.Sort.Order.ASCENDING;
import static org.apache.crunch.lib.Sort.Order.DESCENDING;
import static org.apache.crunch.lib.Sort.ColumnOrder.by;
import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.pairs;

// Crunch version of ch09-mr-features MaxTemperatureUsingSecondarySort
/**
 * 用 Crunch 的排序功能做「二次排序」
 *
 * <p>这是第 9 章 {@code MaxTemperatureUsingSecondarySort} 的 Crunch 版本：
 * 先按「年」升序、再按「气温」降序排一遍，
 * 这样同一个年份的记录会挨在一起，
 * 且 hottest 的那条排在最前面，
 * 取每个分组的头一条就是最高气温
 * <p>Crunch 的 {@code Sort.sortPairs} 直接支持多列排序规则，
 * 不需要自己写 {@code Partitioner} 和 {@code GroupingComparator}
 */
public class MaxTemperatureUsingSecondarySortCrunch {

  /**
   * 作业入口
   *
   * @param args 两个参数：输入目录、输出目录
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // 参数必须是两个
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureUsingSecondarySortCrunch <input path> <output path>");
      // 直接退出
      System.exit(-1);
    }
    // 建立管道
    Pipeline pipeline = new MRPipeline(MaxTemperatureUsingSecondarySortCrunch.class);
    // 读入文本文件
    PCollection<String> records = pipeline.readTextFile(args[0]);
    // 每行变成「年 -> 气温」的整数对
    PCollection<Pair<Integer, Integer>> pairs = records
        .parallelDo(toYearTempValueFn(), pairs(ints(), ints()));

    // 二次排序：先按第 1 列（年）升序，再按第 2 列（气温）降序
    // 两条规则依次生效，后一条在同一年内起作用
    PCollection<Pair<Integer, Integer>> sorted =
        Sort.sortPairs(pairs, by(1, ASCENDING), by(2, DESCENDING));
    // 输出排好序的结果
    pipeline.writeTextFile(sorted, args[1]);
    // 触发作业执行
    pipeline.run();
  }

  /**
   * 造一个把文本行变成「年 -> 气温」整数对的 {@code DoFn}
   *
   * @return 供 {@code parallelDo} 使用的处理函数
   */
  private static DoFn<String, Pair<Integer, Integer>> toYearTempValueFn() {
    return new DoFn<String, Pair<Integer, Integer>>() {
      // 解析器复用
      NcdcRecordParser parser = new NcdcRecordParser();
      /**
       * 处理一行 NCDC 记录
       *
       * @param input 一行文本
       * @param emitter 用来发出结果
       */
      @Override
      public void process(String input, Emitter<Pair<Integer, Integer>> emitter) {
        // 拆成各个字段
        parser.parse(input);
        // 只要温度合格的记录
        if (parser.isValidTemperature()) {
          // 排序要用整数，年份不能是字符串
          emitter.emit(Pair.of(parser.getYearInt(), parser.getAirTemperature()));
        }
      }
    };
  }
}
