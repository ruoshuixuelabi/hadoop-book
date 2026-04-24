package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.join.JoinStrategy;
import org.apache.crunch.lib.join.JoinType;
import org.apache.crunch.lib.join.MapsideJoinStrategy;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// Crunch version of ch09-mr-features MaxTemperatureByStationNameUsingDistributedCacheFile
// Crunch版本的最大气温计算程序,使用Map端连接将气象站名称关联到结果

/**
 * 最大气温计算程序(按气象站名称分组)
 *
 * 演示Crunch中的Map端连接(Mapside Join)操作:
 * - 读取气象站元数据(气象站ID -> 气象站名称)
 * - 连接气象数据与元数据
 * - 按气象站名称分组计算最大气温
 *
 * 与Reduce端连接的区别:
 * - Map端连接在Map阶段完成,需要将一个数据集放入内存
 * - 适用于一个数据集较小的情况(气象站数据)
 * - 无需Shuffle和Sort阶段,效率更高
 *
 * 使用场景:
 * - 气象站数据相对静态,适合广播到所有Map任务
 */
public class MaxTemperatureByStationNameCrunch {

    /**
     * 主程序入口
     *
     * @param args args[0]: NCDC数据输入路径
     *             args[1]: 气象站数据输入路径
     *             args[2]: 输出路径
     */
    public static void main(String[] args) throws Exception {
        // 参数验证
        if (args.length != 3) {
            System.err.println("Usage: MaxTemperatureByStationNameCrunch <ncdc input> <station input> <output>");
            System.exit(-1);
        }
        // 创建Pipeline
        Pipeline pipeline = new MRPipeline(MaxTemperatureByStationNameCrunch.class);

        // 读取NCDC气象数据
        PCollection<String> records = pipeline.readTextFile(args[0]);
        // 读取气象站元数据
        PCollection<String> stations = pipeline.readTextFile(args[1]);

        // 转换气象数据为(气象站ID, 气温)对
        PTable<String, Integer> maxTemps = records
            .parallelDo(toStationIdTempPairsFn(), tableOf(strings(), ints()))
            .groupByKey()
            .combineValues(Aggregators.MAX_INTS());

        // 转换气象站数据为(气象站ID, 气象站名称)对
        PTable<String, String> stationIdToName = stations
            .parallelDo(toStationIdNamePairsFn(), tableOf(strings(), strings()));

        // 创建Map端连接策略
        JoinStrategy<String, Integer, String> mapsideJoin = MapsideJoinStrategy.create();
        // 执行内连接,获取(气象站名称, (最大气温, 气象站名称))对
        PTable<String, Pair<Integer, String>> joined =
            mapsideJoin.join(maxTemps, stationIdToName, JoinType.INNER_JOIN);

        // 写入输出
        pipeline.writeTextFile(joined, args[2]);
        pipeline.run();
    }

    /**
     * 将NCDC记录转换为(气象站ID, 气温)对
     *
     * @return DoFn函数对象
     */
    private static DoFn<String, Pair<String, Integer>> toStationIdTempPairsFn() {
        return new DoFn<String, Pair<String, Integer>>() {
            NcdcRecordParser parser = new NcdcRecordParser();

            @Override
            public void process(String input, Emitter<Pair<String, Integer>> emitter) {
                parser.parse(input);
                if (parser.isValidTemperature()) {
                    emitter.emit(Pair.of(parser.getStationId(), parser.getAirTemperature()));
                }
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
                    emitter.emit(Pair.of