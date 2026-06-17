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
// Crunch版本的数据排序示例,演示如何按气温降序排列

/**
 * 按气温排序程序
 *
 * 演示Crunch中的排序功能:
 * - 按气温降序排列所有记录
 *
 * 注意: 此程序使用TotalOrderPartitioner进行全局排序,
 * 确保输出数据全局有序。
 */
public class SortByTemperatureCrunch {

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

        // 创建Pipeline
        Pipeline pipeline = new MRPipeline(SortByTemperatureCrunch.class);
        PCollection<String> records = pipeline.readTextFile(args[0]);

        // 将记录转换为(气温, 原始记录)对
        PTable<Integer, String> temps = records
            .parallelDo(toTempValuePairsFn(), tableOf(ints(), strings()));

        // 按气温降序排序
        PTable<Integer, String> sorted = Sort.sort(temps, Sort.Order.DESCENDING);

        pipeline.writeTextFile(sorted, args[1]);
        pipeline.run();
    }

    /**
     * 将NCDC记录转换为(气温, 原始记录)对
     *
     * 注意: 键是气温,值是原始记录,这允许我们按气温排序同时保留原始数据
     *
     * @return DoFn函数对象
     */
    private static DoFn<String, Pair<Integer, String>> toTempValuePairsFn() {
        return new DoFn<String, Pair<Integer, String>>() {
            NcdcRecordParser parser = new NcdcRecordParser();

            @Override
            public void process(String input, Emitter<Pair<Integer, String>> emitter) {
                parser.parse(input);
                if (parser.isValidTemperature()) {
                    // 键是气温,用于排序
                    emitter.emit(Pair.of(parser.getAirTemperature(), input));
                }
            }
        };
    }
}