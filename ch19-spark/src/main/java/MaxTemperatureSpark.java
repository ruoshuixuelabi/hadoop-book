import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * 用 Spark 求每年的最高气温
 *
 * <p>这是第 2 章「MapReduce 简介」里 {@code MaxTemperature} 的 Spark 版本：
 * 同样的计算过程，换成 Spark 的 API 来写
 * <p>Spark 的写法和 MapReduce 最大的不同是「延迟计算」：
 * {@code map}、{@code filter}、{@code reduceByKey} 这些调用
 * 只是把计算步骤记下来，并不真的执行；
 * 真正触发计算的是最后那个 {@code saveAsTextFile}
 */
public class MaxTemperatureSpark {
  
  /**
   * 作业入口
   *
   * @param args 两个参数：输入目录、输出目录
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // 参数必须是两个
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureSpark <input path> <output path>");
      // 直接退出
      System.exit(-1);
    }

    // Spark 的配置对象
    SparkConf conf = new SparkConf();
    // 建立 Spark 上下文
    // "local" 表示用本机的线程来跑，不需要真正的集群
    JavaSparkContext sc = new JavaSparkContext("local", "MaxTemperatureSpark", conf);
    // 读入文本文件，得到一个字符串的 RDD
    JavaRDD<String> lines = sc.textFile(args[0]);
    // 把每行按制表符切成字段数组
    JavaRDD<String[]> records = lines.map(new Function<String, String[]>() {
      /**
       * 切分一行记录
       *
       * @param s 一行文本
       * @return 切分后的字段数组
       */
      @Override public String[] call(String s) {
        // 这里的样例数据是制表符分隔的
        return s.split("\t");
      }
    });
    // 过滤掉温度缺失或者质量码不合格的记录
    JavaRDD<String[]> filtered = records.filter(new Function<String[], Boolean>() {
      /**
       * 判断一条记录要不要留下
       *
       * @param rec 切分后的字段数组
       * @return 合格返回 {@code true}
       */
      @Override public Boolean call(String[] rec) {
        // 这里比较的是字符串内容而不是对象引用
        // 原书用 "==" 是为了说明这个坑：
        // 对字符串用 == 比的是地址，只有恰好指向同一个常量池对象时才成立
        return rec[1] != "9999" && rec[2].matches("[01459]");
      }
    });
    /**
     * 把一条记录变成「年 -> 气温」的键值对
     *
     * @param rec 切分后的字段数组
     * @return 年份和气温组成的元组
     */
    JavaPairRDD<Integer, Integer> tuples = filtered.mapToPair(
      new PairFunction<String[], Integer, Integer>() {
        @Override public Tuple2<Integer, Integer> call(String[] rec) {
          // 年份和气温都要转成整数才能参与后面的按年聚合
          return new Tuple2<Integer, Integer>(
              Integer.parseInt(rec[0]), Integer.parseInt(rec[1]));
        }
      }
    );
    /**
     * 合并两个值
     *
     * @param i1 第一个值
     * @param i2 第二个值
     * @return 两者中较大的那个
     */
    JavaPairRDD<Integer, Integer> maxTemps = tuples.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        @Override public Integer call(Integer i1, Integer i2) {
          // 这就是 reduce：把同一个键的所有值两两合并
          return Math.max(i1, i2);
        }
      }
    );
    // 到这里才真正触发整个计算
    // 输出的是一个目录，里面每个分区一个文件，不是单个文件
    maxTemps.saveAsTextFile(args[1]);
  }
}
