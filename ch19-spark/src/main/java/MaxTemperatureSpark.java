/**
 * Chapter 19: Spark - MaxTemperatureSpark
 *
 * 本章演示了Apache Spark的使用方法,使用Spark实现最大气温计算。
 *
 * 主要功能:
 * - 从HDFS读取NCDC气象数据
 * - 解析数据提取年份和气温
 * - 按年份分组计算最大气温
 * - 将结果保存到HDFS
 *
 * 数据格式:
 * 输入数据为制表符分隔的文本文件,格式为:
 * 年份\t气温\t质量标识
 *
 * 示例输入:
 * 1949\t78\t01459
 * 1950\t-11\t01459
 *
 * 算法流程:
 * 1. textFile(): 读取输入文件为RDD
 * 2. map(): 将每行分割为字符串数组
 * 3. filter(): 过滤无效记录(气温为9999或质量标识不符合)
 * 4. mapToPair(): 转换为(年份,气温)键值对
 * 5. reduceByKey(): 按年份分组求最大气温
 * 6. saveAsTextFile(): 保存结果
 *
 * 运行命令:
 * spark-submit --class MaxTemperatureSpark target/spark-examples.jar \
 *   /input/ncdc/all max-temp-output
 *
 * @see <a href="http://hadoopbook.com">Hadoop: The Definitive Guide</a>
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * 最大气温计算程序
 *
 * 使用Apache Spark实现分布式最大气温计算。
 * 对NCDC气象数据进行处理,找出每个年份的最高气温记录。
 *
 * 数据过滤条件:
 * - 气温不等于9999(缺失值)
 * - 质量标识匹配[01459](有效数据)
 *
 * @example
 * 输入: 1949\t78\t01459
 * 输出: (1949, 78)
 */
public class MaxTemperatureSpark {

    /**
     * 主程序入口
     *
     * @param args args[0]: 输入路径
     *             args[1]: 输出路径
     */
    public static void main(String[] args) throws Exception {
        // 参数验证
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureSpark <input path> <output path>");
            System.exit(-1);
        }

        // 创建Spark配置,使用"local"模式运行
        SparkConf conf = new SparkConf();
        // 创建Spark上下文,"local"表示本地运行
        JavaSparkContext sc = new JavaSparkContext("local", "MaxTemperatureSpark", conf);

        // 读取输入文件,每行为一个字符串
        JavaRDD<String> lines = sc.textFile(args[0]);

        // 将每行按制表符分割为字符串数组
        JavaRDD<String[]> records = lines.map(new Function<String, String[]>() {
            @Override
            public String[] call(String s) {
                return s.split("\t");
            }
        });

        // 过滤无效记录:气温为9999或质量标识不符合
        JavaRDD<String[]> filtered = records.filter(new Function<String[], Boolean>() {
            @Override
            public Boolean call(String[] rec) {
                // rec[1]是气温,rec[2]是质量标识
                // 9999表示缺失气温值,质量标识01459表示有效数据
                return rec[1] != "9999" && rec[2].matches("[01459]");
            }
        });

        // 转换为(年份,气温)键值对
        JavaPairRDD<Integer, Integer> tuples = filtered.mapToPair(
            new PairFunction<String[], Integer, Integer>() {
                @Override
                public Tuple2<Integer, Integer> call(String[] rec) {
                    // rec[0]是年份,rec[1]是气温
                    return new Tuple2<Integer, Integer>(
                        Integer.parseInt(rec[0]), Integer.parseInt(rec[1]));
                }
            }
        );

        // 按年份分组计算最大气温
        JavaPairRDD<Integer, Integer> maxTemps = tuples.reduceByKey(
            new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer i1, Integer i2) {
                    return Math.max(i1, i2);
                }
            }
        );

        // 保存结果到输出路径
        maxTemps.saveAsTextFile(args[1]);
    }
}