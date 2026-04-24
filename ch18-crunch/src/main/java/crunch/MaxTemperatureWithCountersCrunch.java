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
// Crunch版本的最大气温计算程序,演示Crunch中计数器的使用

/**
 * 最大气温计算程序(带计数器)
 *
 * 演示Crunch中的计数器使用,用于监控数据质量:
 * - MISSING: 缺失气温值的记录数
 * - MALFORMED: 格式错误的气温记录数
 * - 动态计数器: 按质量等级统计
 *
 * 与Hadoop MapReduce计数器的区别:
 * - 使用Crunch的setStatus()和increment()方法
 * - 通过StageResult.getCounterValue()获取计数器值
 * - 不依赖Hadoop的Counter API,兼容性更好
 */
public class MaxTemperatureWithCountersCrunch {

    /**
     * 计数器枚举
     *
     * 用于跟踪数据质量问题
     */
    enum Temperature {
        MISSING, // 缺失气温值(9999)
        MALFORMED // 格式错误的气温值
    }

    /**
     * 主程序入口
     *
     * @param args args[0]: 输入路径
     *             args[1]: 输出路径
     */
    public static void main(String[] args) throws Exception {
        // 参数验证
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureWithCountersCrunch <input path> <output path>");
            System.exit(-1);
        }

        // 创建Pipeline
        Pipeline pipeline = new MRPipeline(MaxTemperatureWithCountersCrunch.class);
        PCollection<String> records = pipeline.readTextFile(args[0]);

        // 获取输入记录总数(用于计算百分比)
        long total = records.getSize();

        // 转换、聚合并输出
        PTable<String, Integer> maxTemps = records
          .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()))
          .groupByKey()
          .combineValues(Aggregators.MAX_INTS());

        pipeline.writeTextFile(maxTemps, args[1]);

        // 执行管道并获取结果
        PipelineResult result = pipeline.run();
        if (result.succeeded()) {
            // 遍历每个Stage的结果
            for (StageResult stageResult : result.getStageResults()) {
                System.out.println(stageResult.getStageName());
                // TODO: point out that we haven't used the MR API for counters (avoids compat issues)
                // 获取计数器值
                long missing = stageResult.getCounterValue(Temperature.MISSING);
                long malformed = stageResult.getCounterValue(Temperature.MALFORMED);
                System.out.println("Missing: " + missing);
                System.out.println("Malformed: " + malformed);
                System.out.println("Total: " + total);
                System.out.printf("Records with missing temperature fields: %.2f%%\n",
                    100.0 * missing / total);
            }
        }
    }

    /**
     * 创建数据转换函数
     *
     * 解析NCDC记录,输出(年份, 气温)对,并更新计数器
     *
     * @return DoFn函数对象
     */
    private static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
        return new DoFn<String, Pair<String, Integer>>() {
            NcdcRecordParser parser = new NcdcRecordParser();

            @Override
            public void process(String input, Emitter<Pair<String, Integer>> emitter) {
                parser.parse(input);
                if (parser.isValidTemperature()) {
                    // 有效数据,输出
                    emitter.emit(Pair.of(parser.getYear(), parser.getAirTemperature()));
                } else if (parser.isMalformedTemperature()) {
                    // 格式错误,设置状态信息并增加计数器
                    setStatus("Ignoring possibly corrupt input: " + input);
                    increment(Temperature.MALFORMED);
                } else if (parser.isMissingTemperature()) {
                    // 缺失值,增加计数器
                    increment(Temperature.MISSING);
                }

                // 动态计数器:按质量等级统计
                // 这会自动创建名为"TemperatureQuality"的计数器组
                increment("TemperatureQuality", parser.getQuality(), 1);
            }
        };
    }

}