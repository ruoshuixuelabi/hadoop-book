package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// Crunch version of ch12-avro MaxTemperatureWithCompression
// Crunch版本的最大气温计算程序,支持Gzip压缩输出

/**
 * 最大气温计算程序(带压缩)
 *
 * 演示Crunch中如何配置MapReduce作业的压缩选项:
 * - 启用输出压缩
 * - 设置压缩编码器(GzipCodec)
 *
 * 压缩配置:
 * - mapreduce.output.fileoutputformat.compress: 启用输出压缩
 * - mapreduce.output.fileoutputformat.compress.codec: 压缩编码器类
 */
public class MaxTemperatureWithCompressionCrunch {

    /** 缺失气温标记值 */
    private static final int MISSING = 9999;

    /**
     * 主程序入口
     *
     * @param args args[0]: 输入路径
     *             args[1]: 输出路径
     */
    public static void main(String[] args) throws Exception {
        // 参数验证
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureWithCompressionCrunch <input path> <output path>");
            System.exit(-1);
        }

        // 创建Hadoop配置
        Configuration conf = new Configuration();
        // 启用输出压缩
        conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
        // 设置Gzip压缩编码器
        conf.setClass("mapreduce.output.fileoutputformat.compress.codec", GzipCodec.class, CompressionCodec.class);

        // 使用自定义配置创建Pipeline
        Pipeline pipeline = new MRPipeline(MaxTemperatureWithCompressionCrunch.class, conf);
        PCollection<String> records = pipeline.readTextFile(args[0]);

        // 转换并聚合
        PTable<String, Integer> maxTemps = records
          .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()))
          .groupByKey()
          .combineValues(Aggregators.MAX_INTS());

        pipeline.writeTextFile(maxTemps, args[1]);
        pipeline.run();
    }

    /**
     * 创建数据转换函数
     *
     * 内联实现DoFn,不使用NcdcRecordParser,演示手动解析逻辑
     *
     * @return DoFn函数对象
     */
    private static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
        return new DoFn<String, Pair<String, Integer>>() {
            @Override
            public void process(String input, Emitter<Pair<String, Integer>> emitter) {
                String line = input.toString();
                // 从行中提取年份(位置15-19)
                String year = line.substring(15, 19);
                int airTemperature;
                // 处理带加号的气温值(parseInt不支持前导加号)
                if (line.charAt(87) == '+') {
                    airTemperature = Integer.parseInt(line.substring(88, 92));
                } else {
                    airTemperature = Integer.parseInt(line.substring(87, 92));
                }
                // 提取质量标识
                String quality = line.substring(92, 93);
                // 过滤无效数据: 非缺失值 且 质量标识有效
                if (airTemperature != MISSING && quality.matches("[01459]")) {