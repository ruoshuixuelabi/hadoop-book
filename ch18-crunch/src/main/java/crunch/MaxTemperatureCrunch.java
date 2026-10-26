package crunch;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

import org.apache.crunch.PipelineResult;
import org.apache.crunch.fn.Aggregators;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;

// Crunch version of ch02-mr-intro MaxTemperature
/**
 * 用 Crunch 求每年的最高气温
 *
 * <p>这是第 2 章「MapReduce 简介」里 {@code MaxTemperature} 的 Crunch 版本：
 * 同样的计算过程，换成 Crunch 的 API 来写
 * <p>Crunch 用「管道」的思路来表达 MapReduce：
 * 先读入数据得到一个 {@code PCollection}，
 * 再一步步做转换，最后输出
 * <p>和手写 MapReduce 相比，
 * 这里看不到 {@code Mapper} / {@code Reducer}，
 * 取而代之的是 {@code parallelDo} 加上一组「组合子」
 */
public class MaxTemperatureCrunch {
  
  /**
   * 作业入口
   *
   * @param args 两个参数：输入目录、输出目录
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // 参数必须是两个，否则没法往下走
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureCrunch <input path> <output path>");
      // 直接退出，不打印堆栈
      System.exit(-1);
    }

    // 建立管道；MRPipeline 表示这个作业跑在 MapReduce 上
    Pipeline pipeline = new MRPipeline(MaxTemperatureCrunch.class);
    // 读入文本文件，得到一个 PCollection
    PCollection<String> records = pipeline.readTextFile(args[0]);

    // 把每行变成「年 -> 气温」的键值对
    // 第二个参数是输出表的类型说明：键是字符串，值是整数
    PTable<String, Integer> yearTemperatures = records
        .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()));
    // 按年分组，再用内置的最大值组合子求出每年的最高气温
    // 这一步就相当于 MapReduce 里的 groupByKey + reduce
    PTable<String, Integer> maxTemps = yearTemperatures
      .groupByKey()
      .combineValues(Aggregators.MAX_INTS());

    // 把结果写成文本文件
    maxTemps.write(To.textFile(args[1]));
    // done() 会真正触发作业执行并等它跑完
    PipelineResult result = pipeline.done();
    // 用退出码告诉外面作业成功还是失败
    System.exit(result.succeeded() ? 0 : 1);
  }

  /**
   * 造一个把文本行变成「年 -> 气温」键值对的 {@code DoFn}
   *
   * <p>{@code DoFn} 是 Crunch 里最基础的「处理函数」：
   * 进来一条数据，往 {@code Emitter} 里发出若干条新数据
   *
   * @return 供 {@code parallelDo} 使用的处理函数
   */
  static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
    return new DoFn<String, Pair<String, Integer>>() {
      // 解析器要复用，不能每行都新建一个
      NcdcRecordParser parser = new NcdcRecordParser();
      /**
       * 处理一行 NCDC 记录
       *
       * @param input 一行文本
       * @param emitter 用来发出结果
       */
      @Override
      public void process(String input, Emitter<Pair<String, Integer>> emitter) {
        // 先把定长格式的行拆成各个字段
        parser.parse(input);
        // 温度缺失或质量码不合格的记录直接丢掉
        if (parser.isValidTemperature()) {
          // 只发出合格的温度
          emitter.emit(Pair.of(parser.getYear(), parser.getAirTemperature()));
        }
      }
    };
  }

}
