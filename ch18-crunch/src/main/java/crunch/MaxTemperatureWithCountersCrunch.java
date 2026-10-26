package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// Crunch version of ch09-mr-features MaxTemperatureWithCountersCrunch and MissingTemperatureFields
// Note that both are naturally combined into a single program.
/**
 * 用 Crunch 求每年的最高气温，同时统计温度缺失和格式错误的记录
 *
 * <p>这是第 9 章 {@code MaxTemperatureWithCounters}
 * 和 {@code MissingTemperatureFields} 两个示例的合并版：
 * 前者用计数器统计「有多少条记录被丢掉」，后者统计「丢掉的记录是哪些」
 * <p>Crunch 里用 {@code setStatus} 和 {@code increment} 来记录这些信息，
 * 作业跑完后从 {@code StageResult} 里把计数器的值读出来
 */
public class MaxTemperatureWithCountersCrunch {
  
  /**
   * 计数器组的名字
   *
   * <p>一个枚举项就是一个独立的计数器，
   * 这样在作业结束时可以按名字分别读出各项的计数
   */
  enum Temperature {
    // 温度字段缺失（NCDC 用 9999 表示）
    MISSING,
    // 温度字段格式不对（连正负号都没有）
    MALFORMED
  }

  /**
   * 作业入口
   *
   * @param args 两个参数：输入目录、输出目录
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // 参数必须是两个
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCountersCrunch <input path> <output path>");
      // 直接退出
      System.exit(-1);
    }

    // 建立管道
    Pipeline pipeline = new MRPipeline(MaxTemperatureWithCountersCrunch.class);
    // 读入文本文件
    PCollection<String> records = pipeline.readTextFile(args[0]);
    // 总记录数，用来算百分比
    // 有了分母，后面才能算出「缺失比例」
    long total = records.getSize();
    
    // 按年分组求最大值
    // 被丢掉的记录由 toYearTempPairsFn 里的计数器负责统计
    PTable<String, Integer> maxTemps = records
      .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()))
      .groupByKey()
      .combineValues(Aggregators.MAX_INTS());
    
    // 结果写成文本文件
    pipeline.writeTextFile(maxTemps, args[1]);
    // 跑完作业，拿到运行结果
    PipelineResult result = pipeline.run();
    // 只有成功才去读计数器
    if (result.succeeded()) {
      // 一个作业可能分几个阶段，逐个阶段看
      for (StageResult stageResult : result.getStageResults()) {
        // 先把阶段名打出来
        System.out.println(stageResult.getStageName());
        // TODO: point out that we haven't used the MR API for counters (avoids compat issues)
        // 温度缺失的记录数
        long missing = stageResult.getCounterValue(Temperature.MISSING);
        // 温度格式错误的记录数
        long malformed = stageResult.getCounterValue(Temperature.MALFORMED);
        // 打印计数
        System.out.println("Missing: " + missing);
        // 打印计数
        System.out.println("Malformed: " + malformed);
        // 打印总数
        System.out.println("Total: " + total);
        // 算成百分比
        // 先乘 100.0 再除，避免整数除法把小数部分抹掉
        System.out.printf("Records with missing temperature fields: %.2f%%\n",
            100.0 * missing / total);
      }
    }
  }

  /**
   * 造一个把文本行变成「年 -> 气温」键值对的 {@code DoFn}
   *
   * <p>除了发出合格的数据，
   * 它还负责把不合格的数据分类计数
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
        // 温度合格，正常发出
        if (parser.isValidTemperature()) {
          // 键是年份，值是气温
          emitter.emit(Pair.of(parser.getYear(), parser.getAirTemperature()));
        // 温度字段格式不对
        } else if (parser.isMalformedTemperature()) {
          // 记一笔状态，方便在作业日志里定位问题数据
          setStatus("Ignoring possibly corrupt input: " + input);
          // 计数器加一
          increment(Temperature.MALFORMED);
        // 温度字段缺失（9999）
        } else if (parser.isMissingTemperature()) {
          // 计数器加一
          increment(Temperature.MISSING);
        }

        // dynamic counter
        // 动态计数器：名字是「组名 + 质量码」
        // 这样可以分别统计各种质量码各有多少条
        increment("TemperatureQuality", parser.getQuality(), 1);
      }
    };
  }

}
