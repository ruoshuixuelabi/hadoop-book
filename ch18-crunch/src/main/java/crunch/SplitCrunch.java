package crunch;

import java.io.Serializable;
import org.apache.crunch.FilterFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;

/**
 * 数据分割示例
 *
 * 演示如何使用Crunch的FilterFn将数据分割为"好"和"坏"两部分:
 * - goodRecords: 有效的气温记录
 * - badRecords: 无效的气温记录
 *
 * 这在数据质量分析中很有用,可以分别处理有效和无效数据。
 */
public class SplitCrunch implements Serializable {

    /**
     * 主程序入口
     *
     * @param args 无需参数,使用硬编码的输入输出路径
     */
    public static void main(String[] args) throws Exception {
        // 创建Pipeline
        Pipeline pipeline = new MRPipeline(SplitCrunch.class);

        // 读取包含损坏记录的输入文件
        PCollection<String> records = pipeline.readTextFile("input/ncdc/micro-tab/sample_corrupt.txt");

        // 过滤出有效记录
        PCollection<String> goodRecords = records.filter(new FilterFn<String>() {
            NcdcRecordParser parser = new NcdcRecordParser();

            /**
             * 接受有效的气温记录
             *
             * @param s 输入记录
             * @return true如果记录有效
             */
            @Override
            public boolean accept(String s) {
                parser.parse(s);
                return parser.isValidTemperature();
            }
        });

        // 过滤出无效记录
        PCollection<String> badRecords = records.filter(new FilterFn<String>() {
            NcdcRecordParser parser = new NcdcRecordParser();

            /**
             * 接受无效的气温记录
             *
             * @param s 输入记录
             * @return true如果记录无效
             */
            @Override
            public boolean accept(String s) {
                parser.parse(s);
                return !parser.isValidTemperature();
            }
        });

        // 分别写入不同输出目录
        pipeline.writeTextFile(goodRecords, "output-good");
        pipeline.writeTextFile(badRecords, "output-bad");
        pipeline.run();
    }
}