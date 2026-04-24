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
// Crunch版本的最大气温计算程序,演示如何使用二次排序

/**
 * 最大气温计算程序(二次排序)
 *
 * 演示Crunch中的排序功能:
 * - 按第一列(年份)升序排列
 * - 按第二列(气温)降序排列
 *
 * 这意味着数据会先按年份分组,年份内按气温从高到低排序。
 *
 * 二次排序的应用场景:
 * - 找出每个类别中的Top N
 * - 关联排序与原始值
 */
public class MaxTemperatureUsingSecondarySortCrunch {

    /**
     * 主程序入口
     *
     * @param args args[0]: 输入路径
     *             args[1]: 输出路径
     */
    public static void main(String[] args) throws Exception {
        // 参数验证
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureUsingSecondarySortCrunch <input path> <output path>");
            System.exit(-1);
        }

        // 创建Pipeline
        Pipeline pipeline = new MRPipeline(MaxTemperatureUsingSecondarySortCrunch.class);
        PCollection<String> records = pipeline.readTextFile(args[0]);

        // 将记录转换为(年份, 气温)对
        PCollection<Pair<Integer, Integer>> pairs = records
            .parallelDo(toYearTempValueFn(), pairs(ints(), ints()));

        // 按年份升序、气温暖序排序
        PCollection<Pair<Integer, Integer>> sorted =
            Sort.sortPairs(pairs, by(1, ASCENDING), by(2, DESCENDING));

        pipeline.writeTextFile(sorted, args[1]);
        pipeline.run();
    }

    /**
     * 将NCDC记录转换为(年份, 气温)对
     *
     * @return DoFn函数对象
     */
    private static DoFn<String, Pair<Integer, Integer>> toYearTempValueFn() {
        return new DoFn<String, Pair<Integer, Integer>>() {
            NcdcRecordParser parser = new NcdcRecordParser();

            @Override
            public void process(String input, Emitter<Pair<Integer, Integer>> emitter) {
                parser.parse(input);
                if (parser.isValidTemperature()) {
                    emitter.emit(Pair.of(parser.getYearInt(), parser.getAirTemperature()));
                }
            }
        };
    }
}