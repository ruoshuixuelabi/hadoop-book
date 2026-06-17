package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Join;

import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// Crunch version of ch09-mr-features JoinRecordWithStationName
// Crunch版本的数据连接示例,演示如何连接气象数据和气象站名称

/**
 * 气象数据与气象站名称连接程序
 *
 * 演示Crunch中的表连接操作:
 * - 读取两个数据源(气象数据和气象站元数据)
 * - 将两个PTable按气象站ID连接
 * - 输出连接后的结果
 *
 * 连接类型: 内连接(INNER JOIN)
 * - 只保留两个表中都存在的键
 *
 * 与Map端连接的区别:
 * - 使用Join.join()进行Reduce端连接
 * - 两个数据集都较大时使用
 * - 需要Shuffle阶段
 */
public class JoinRecordWithStationNameCrunch {

    /**
     * 主程序入口
     *
     * @param args args[0]: NCDC气象数据输入路径
     *             args[1]: 气象站元数据输入路径
     *             args[2]: 输出路径
     */
    public static void main(String[] args) throws Exception {
        // 参数验证
        if (args.length != 3) {
            System.err.println("Usage: JoinRecordWithStationNameCrunch <ncdc input> <station input> <output>");
            System.exit(-1);
        }
        // 创建Pipeline
        Pipeline pipeline = new MRPipeline(JoinRecordWithStationNameCrunch.class);

        // 读取NCDC气象数据
        PCollection<String> records = pipeline.readTextFile(args[0]);
        // 读取气象站元数据
        PCollection<String> stations = pipeline.readTextFile(args[1]);

        // 将气象数据转换为(气象站ID, 原始记录)对
        PTable<String, String> stationIdToRecord = records
            .parallelDo(toStationIdRecordPairsFn(), tableOf(strings(), strings()));

        // 将气象站元数据转换为(气象站ID, 气象站名称)对
        PTable<String, String> stationIdToName = stations
            .parallelDo(toStationIdNamePairsFn(), tableOf(strings(), strings()));

        // 执行内连接
        PTable<String, Pair<String, String>> joined =
            Join.join(stationIdToRecord, stationIdToName);

        // 写入输出
        pipeline.writeTextFile(joined, args[2]);
        pipeline.run();
    }

    /**
     * 将NCDC记录转换为(气象站ID, 原始记录)对
     *
     * @return DoFn函数对象
     */
    private static DoFn<String, Pair<String, String>> toStationIdRecordPairsFn() {
        return new DoFn<String, Pair<String, String>>() {
            NcdcRecordParser parser = new NcdcRecordParser();

            @Override
            public void process(String input, Emitter<Pair<String, String>> emitter) {
                parser.parse(input);
                // 气象站ID作为键,原始记录作为值
                emitter.emit(Pair.of(parser.getStationId(), input));
            }
        };
    }

    /**
     * 将气象站元数据转换为(气象站ID, 气象站名称)对
     *
     * @return DoFn函数对象
     */
    private static DoFn<String, Pair<String, String>> toStationIdNamePairsFn() {
        return new DoFn<String, Pair<String, String>>() {
            NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

            @Override
            public void process(String input, Emitter<Pair<String, String>> emitter) {
                if (parser.parse(input)) {
                    emitter.emit(Pair.of(parser.getStationId(), parser.getStationName()));
                }
            }
        };
    }
}