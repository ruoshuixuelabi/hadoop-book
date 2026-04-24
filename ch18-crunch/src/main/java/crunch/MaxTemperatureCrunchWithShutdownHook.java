package crunch;

// Crunch version of ch02-mr-intro MaxTemperature
// Crunch版本的基础最大气温计算程序,使用异步执行和关闭钩子

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

/**
 * 最大气温计算程序(带关闭钩子)
 *
 * 演示Crunch的异步执行和优雅关闭。
 * 使用runAsync()启动异步作业,并通过JVM关闭钩子确保管道被正确终止。
 *
 * 与MaxTemperatureCrunch的区别:
 * - 使用runAsync()异步执行,可以注册关闭钩子
 * - 更好地控制长时间运行的作业
 *
 * 使用场景:
 * - 在YARN上运行的作业需要在Application Master被kill时清理
 * - 需要在作业运行时执行其他任务
 */
public class MaxTemperatureCrunchWithShutdownHook {

    /**
     * 主程序入口
     *
     * @param args args[0]: 输入路径
     *             args[1]: 输出路径
     */
    public static void main(String[] args) throws Exception {
        // 参数验证
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureCrunchWithShutdownHook <input path> <output path>");
            System.exit(-1);
        }

        // 创建Pipeline
        Pipeline pipeline = new MRPipeline(MaxTemperatureCrunchWithShutdownHook.class);

        // 读取输入数据
        PCollection<String> records = pipeline.readTextFile(args[0]);

        // 转换并聚合
        PTable<String, Integer> yearTemperatures = records
            .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()));
        PTable<String, Integer> maxTemps = yearTemperatures
          .groupByKey()
          .combineValues(Aggregators.MAX_INTS());

        // 写入输出
        pipeline.writeTextFile(maxTemps, args[1]);

        // 异步执行管道
        final PipelineExecution execution = pipeline.runAsync();

        // 注册JVM关闭钩子,在进程被强制终止时Kill管道
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              System.out.println("Stopping pipeline...");
              execution.kill(); // 停止管道
              execution.waitUntilDone(); // 等待停止完成
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }));

        // 等待执行完成
        execution.waitUntilDone();
        // 根据执行结果退出
        System.exit(execution.getResult().succeeded() ? 0 : 1);
    }

    /**
     * 创建数据转换函数
     *
     * 将NCDC记录转换为(年份, 气温)键值对
     *
     * @return DoFn函数对象
     */
    static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
        return new DoFn<String, Pair<String, Integer>>() {
            NcdcRecordParser parser = new NcdcRecordParser();

            @Override
            public void process(String input, Emitter<Pair<String, Integer>> emitter) {
                parser.parse(input);
                if (parser.isValidTemperature()) {
                    emitter.emit(Pair.of(parser.getYear(), parser.getAirTemperature()));
                }
            }
        };
    }

}