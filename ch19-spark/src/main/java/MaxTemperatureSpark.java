/**
 * 文件：MaxTemperatureSpark.java<br>
 * 路径：src/main/java/MaxTemperatureSpark.java<br>
 * 描述：使用Apache Spark计算最大气温程序<br>
 * <br>
 * 功能说明：<br>
 * 本章演示了Apache Spark的使用方法,使用Spark实现最大气温计算。<br>
 * 主要功能：<br>
 * - 从HDFS读取NCDC气象数据<br>
 * - 解析数据提取年份和气温<br>
 * - 按年份分组计算最大气温<br>
 * - 将结果保存到HDFS<br>
 * <br>
 * 数据格式：<br>
 * 输入数据为制表符分隔的文本文件,格式为：<br>
 * 年份\t气温\t质量标识<br>
 * 示例输入：<br>
 * 1949\t78\t01459<br>
 * 1950\t-11\t01459<br>
 * <br>
 * 算法流程：<br>
 * 1. textFile(): 读取输入文件为RDD<br>
 * 2. map(): 将每行分割为字符串数组<br>
 * 3. filter(): 过滤无效记录（气温为9999或质量标识不符合）<br>
 * 4. mapToPair(): 转换为(年份,气温)键值对<br>
 * 5. reduceByKey(): 按年份分组求最大气温<br>
 * 6. saveAsTextFile(): 保存结果<br>
 * <br>
 * 运行命令：<br>
 * spark-submit --class MaxTemperatureSpark target/spark-examples.jar \<br>
 * /input/ncdc/all max-temp-output<br>
 *
 * @see <a href="http://hadoopbook.com">Hadoop: The Definitive Guide</a><br>
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Objects;

/**
 * 最大气温计算程序<br>
 * 使用Apache Spark实现分布式最大气温计算。<br>
 * 对NCDC气象数据进行处理,找出每个年份的最高气温记录。<br>
 * <br>
 * 数据过滤条件：<br>
 * - 气温不等于9999（缺失值）<br>
 * - 质量标识匹配[01459]（有效数据）<br>
 *
 * @example 输入: 1949\t78\t01459<br>
 * 输出: (1949, 78)<br>
 */
public class MaxTemperatureSpark {
    /**
     * 主程序入口<br>
     *
     * @param args args[0]: 输入路径<br>
     *             args[1]: 输出路径
     */
    public static void main(String[] args) throws Exception {
        // 参数验证：确保传入两个参数（输入路径和输出路径）
        if (args.length != 2) {
            // 打印错误信息到标准错误流
            System.err.println("Usage: MaxTemperatureSpark <input path> <output path>");
            // 非正常退出，状态码-1表示错误
            System.exit(-1);
        }
        // 创建Spark配置对象
        SparkConf conf = new SparkConf();
        // 创建Spark上下文，"local"表示本地运行模式
        // JavaSparkContext是Java版Spark程序的入口点
        JavaSparkContext sc = new JavaSparkContext("local", "MaxTemperatureSpark", conf);
        // 读取输入文件，每行为一个字符串创建JavaRDD
        JavaRDD<String> lines = sc.textFile(args[0]);
        // 将每行按制表符分割为字符串数组
        // 使用匿名内部类实现Function接口
        JavaRDD<String[]> records = lines.map(new Function<String, String[]>() {
            @Override
            public String[] call(String s) {
                // split方法按制表符分割字符串
                return s.split("\t");
            }
        });
        // 过滤无效记录：气温为9999或质量标识不符合
        // 使用匿名内部类实现Function接口
        JavaRDD<String[]> filtered = records.filter(new Function<String[], Boolean>() {
            @Override
            public Boolean call(String[] rec) {
                // rec[1]是气温字段，rec[2]是质量标识字段
                // Objects.equals用于安全比较字符串，避免空指针
                // 9999表示缺失气温值，质量标识01459表示有效数据
                return !Objects.equals(rec[1], "9999") && rec[2].matches("[01459]");
            }
        });
        // 转换为(年份,气温)键值对
        // 使用mapToPair和PairFunction接口
        JavaPairRDD<Integer, Integer> tuples = filtered.mapToPair(
                new PairFunction<String[], Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(String[] rec) {
                        // rec[0]是年份，rec[1]是气温
                        // 将字符串转换为Integer类型
                        return new Tuple2<Integer, Integer>(
                                Integer.parseInt(rec[0]), Integer.parseInt(rec[1]));
                    }
                }
        );
        // 按年份分组计算最大气温
        // reduceByKey将相同年份的记录聚合，取最大值
        JavaPairRDD<Integer, Integer> maxTemps = tuples.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        // Math.max返回两个整数中的最大值
                        return Math.max(i1, i2);
                    }
                }
        );
        // 保存结果到输出路径
        // saveAsTextFile将RDD保存为文本文件，每个元素一行
        maxTemps.saveAsTextFile(args[1]);
    }
}