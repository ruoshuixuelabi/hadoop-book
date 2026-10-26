package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// Crunch version of ch02-mr-intro MaxTemperature
/**
 * 用 Crunch 求每年的最高气温，并在作业跑的时候挂一个关闭钩子
 *
 * <p>计算部分和 {@link MaxTemperatureCrunch} 完全一样，
 * 区别是这里用 {@code runAsync()} 异步启动作业，
 * 并注册一个 {@code ShutdownHook}
 * <p>关闭钩子会在进程收到中断信号时执行：
 * 先把还在跑的作业杀掉，再等它收尾，
 * 这样就不会在集群里留下一个半死不活的作业
 */
public class MaxTemperatureCrunchWithShutdownHook {
  
  /**
   * 作业入口
   *
   * @param args 两个参数：输入目录、输出目录
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // 参数必须是两个
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureCrunchWithShutdownHook <input path> <output path>");
      // 直接退出
      System.exit(-1);
    }

    // 建立管道
    Pipeline pipeline = new MRPipeline(MaxTemperatureCrunchWithShutdownHook.class);
    // 读入文本文件
    PCollection<String> records = pipeline.readTextFile(args[0]);

    // 每行变成「年 -> 气温」
    PTable<String, Integer> yearTemperatures = records
        .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()));
    // 按年分组求最大值
    PTable<String, Integer> maxTemps = yearTemperatures
      .groupByKey()
      .combineValues(Aggregators.MAX_INTS());
    
    // 结果写成文本文件
    pipeline.writeTextFile(maxTemps, args[1]);
    // 异步启动作业：立刻返回执行句柄，不等它跑完
    // 这样后面才有余地去注册关闭钩子
    final PipelineExecution execution = pipeline.runAsync();
    // 注册一个关闭钩子
    // 进程被 Ctrl-C 或 kill 时，它会负责把作业收拾干净
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      /**
       * 关闭钩子的具体动作
       */
      @Override
      public void run() {
        try {
          // 告诉用户作业正在被停掉
          System.out.println("Stopping pipeline...");
          // 先杀掉还在跑的作业
          // 通知 MapReduce 作业停止
          // 通知 MapReduce 作业停止
          execution.kill();
          // 等作业真正收尾，避免残留任务
          execution.waitUntilDone();
        // 等待被中断了
        } catch (InterruptedException e) {
          // 打印一下，反正进程马上就要退出了
          e.printStackTrace();
        }
      }
    }));
    // 正常情况下在这里等作业跑完
    execution.waitUntilDone();
    // 用退出码告诉外面作业成功还是失败
    System.exit(execution.getResult().succeeded() ? 0 : 1);
  }

  /**
   * 造一个把文本行变成「年 -> 气温」键值对的 {@code DoFn}
   *
   * @return 供 {@code parallelDo} 使用的处理函数
   */
  static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
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

}
