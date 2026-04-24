package crunch;

/**
 * Chapter 18: Crunch - MaxTemperatureCrunch
 *
 * Crunch版本的基础最大气温计算程序。
 *
 * 本程序演示了Crunch库的基本用法:
 * - 从文本文件读取数据
 * - 使用DoFn进行数据转换
 * - 使用Aggregators进行聚合操作
 * - 将结果写入文本文件
 *
 * 数据流程:
 * 输入文件 -> readTextFile() -> parallelDo() -> groupByKey() -> combineValues() -> writeTextFile()
 *
 * 对应MapReduce流程:
 * InputFormat -> Mapper -> GroupByKey -> Reducer -> OutputFormat
 */

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

/**
 * 最大气温计算程序
 *
 * 读取NCDC气象数据,按年份计算每个年份的最高气温。
 *
 * 使用Crunch的MRPipeline执行MapReduce作业:
 * - parallelDo: 类似于Hadoop的Map
 * - groupByKey + combineValues: 类似于Hadoop的Reduce
 *
 * @example
 * 输入: (1949, 78), (1949, 82), (1950, -11)
 * 输出: (1949, 82), (1950, -11)
 */
public class MaxTemperatureCrunch {

    /**
     * 主程序入口
     *
     * @param args args[0]: 输入路径
     *             args[1]: 输出路径
     */
    public static void main(String[] args) throws Exception {
        // 参数验证
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureCrunch <input path> <output path>");
            System.exit(-1);
        }

        // 创建MRPipeline,指定主类用于获取配置
        Pipeline pipeline = new MRPipeline(MaxTemperatureCrunch.class);

        // 从文本文件读取数据,每行为一个字符串
        PCollection<String> records = pipeline.readTextFile(args[0]);

        // 将记录转换为(年份, 气温)键值对
        PTable<String, Integer> yearTemperatures = records.parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()));

        // 按年份分组并计算最大值
        PTable<String, Integer> maxTemps = yearTemperatures.groupByKey().combineValues(Aggregators.MAX_INTS());

        // 将结果写入文本文件
        maxTemps.write(To.textFile(args[1]));

        // 等待管道执行完成并获取结果
        PipelineResult result = pipeline.done();
        System.exit(result.succeeded() ? 0 : 1);
    }

    /**
     * 创建数据转换函数
     *
     * 将NCDC记录行解析为(年份, 气温)键值对
     *
     * @return DoFn函数对象
     */
    static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
        return new DoFn<String, Pair<String, Integer>>() {
            // NCDC记录解析器,每个DoFn实例复用以提高效率
            NcdcRecordParser parser = new NcdcRecordParser();

            /**
             * 处理每条输入记录
             *
             * @param input NCDC数据行
             * @param emitter 发射器,用于输出(年份, 气温)对
             */
            @Override
            public void process(String input, Emitter<Pair<String, Integer>> emitter) {
                parser.parse(input);
                // 只输出有效的气温记录
                if (parser.isValidTemperature()) {
                    emitter.emit(Pair.of(parser.getYear(), parser.getAirTemperature()));
                }
            }
        };
    }
}