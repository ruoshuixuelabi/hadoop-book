package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Sort;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// Crunch version of ch09-mr-features SortByTemperatureUsingTotalOrderPartitioner
/**
 * 用 Crunch 把观测记录按气温从高到低排序
 *
 * <p>这是第 9 章 {@code SortByTemperatureUsingTotalOrderPartitioner} 的 Crunch 版本：
 * 原例子里要自己写 {@code TotalOrderPartitioner} 才能做全序排序，
 * 而 Crunch 直接提供了 {@code Sort.sort}，
 * 排序规则由框架负责
 */
public class SortByTemperatureCrunch {

  /**
   * 作业入口
   *
   * @param args 两个参数：输入目录、输出目录
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // 参数必须是两个
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureCrunch <input path> <output path>");
      // 直接退出
      System.exit(-1);
    }
    // 建立管道
    Pipeline pipeline = new MRPipeline(SortByTemperatureCrunch.class);
    // 读入文本文件
    PCollection<String> records = pipeline.readTextFile(args[0]);
    // 每行变成「气温 -> 原始行」的键值对
    // 键用气温，排序就按这个键来
    PTable<Integer, String> temps = records
        .parallelDo(toTempValuePairsFn(), tableOf(ints(), strings()));

    // 按键降序排序，也就是气温从高到低
    PTable<Integer, String> sorted = Sort.sort(temps, Sort.Order.DESCENDING);
    // 输出排好序的结果
    pipeline.writeTextFile(sorted, args[1]);
    // 触发作业执行
    pipeline.run();
  }

  /**
   * 造一个把文本行变成「气温 -> 原始行」键值对的 {@code DoFn}
   *
   * @return 供 {@code parallelDo} 使用的处理函数
   */
  private static DoFn<String, Pair<Integer, String>> toTempValuePairsFn() {
    return new DoFn<String, Pair<Integer, String>>() {
      // 解析器复用
      NcdcRecordParser parser = new NcdcRecordParser();
      /**
       * 处理一行 NCDC 记录
       *
       * @param input 一行文本
       * @param emitter 用来发出结果
       */
      @Override
      public void process(String input, Emitter<Pair<Integer, String>> emitter) {
        // 拆成各个字段
        parser.parse(input);
        // 只要温度合格的记录
        if (parser.isValidTemperature()) {
          // 键是气温，值保留原始行
          emitter.emit(Pair.of(parser.getAirTemperature(), input));
        }
      }
    };
  }
}
