/**
 * Chapter 19: Spark - SimpleTest
 *
 * 本章演示了Apache Spark的基本操作和单元测试方法。
 *
 * 主要测试内容:
 * - RDD创建和缓存
 * - 过滤(filter)操作
 * - 计数(count)操作
 * - RDD到PairRDD的转换
 * - dump工具方法用于调试输出
 *
 * 运行命令:
 * mvn test -pl ch19-spark
 */

import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Tuple2;

import static org.junit.Assert.assertEquals;

/**
 * Spark简单测试类
 *
 * 演示Spark的基本API使用方法和单元测试技巧。
 * 测试包括RDD创建、转换操作和行动操作。
 *
 * 注意: 由于Spark的Function需要序列化,测试类实现了Serializable接口
 */
public class SimpleTest implements Serializable { // 需要序列化,因为Spark的Function需要通过网络传输

    /** 临时文件夹规则,测试结束后自动清理 */
    @Rule
    public transient TemporaryFolder tmpDir = new TemporaryFolder();

    /**
     * 测试Spark基本功能
     *
     * 测试流程:
     * 1. 创建临时输入文件
     * 2. 创建Spark上下文
     * 3. 读取文件创建RDD
     * 4. 缓存RDD
     * 5. 过滤偶数长度的字符串
     * 6. 计数并验证结果
     *
     * @throws IOException 如果文件操作失败
     */
    @Test
    public void test() throws IOException {
        // 创建临时输入文件
        File inputFile = tmpDir.newFile("input");
        // 从classpath复制fruit.txt到临时文件
        Files.copy(Resources.newInputStreamSupplier(Resources.getResource("fruit.txt")),
            inputFile);

        // 创建Spark配置,使用local模式运行
        SparkConf conf = new SparkConf();
        // 创建Spark上下文
        JavaSparkContext sc = new JavaSparkContext("local", "Simple App", conf);

        // 读取文本文件并缓存(避免重复计算)
        JavaRDD<String> a = sc.textFile(inputFile.getPath()).cache();

        // 打印RDD内容(用于调试)
        dump(a);

        // 过滤偶数长度的字符串并计数
        long numEven = a.filter(new Function<String, Boolean>() {
            public Boolean call(String input) {
                return input.length() % 2 == 0; // 偶数长度
            }
        }).count();

        // 验证结果: fruit.txt中应该有2个偶数长度的单词
        // cherry(6), apple(5), banana(6) -> cherry和banana是偶数
        assertEquals(2, numEven);

        // 停止Spark上下文,释放资源
        sc.stop();
    }

    /**
     * 将JavaRDD转换为字符串表示
     *
     * 用于调试和日志输出,格式为{element1,element2,...}
     *
     * @param rdd 输入的RDD
     * @return RDD内容的字符串表示
     */
    <T> String dump(JavaRDD<T> rdd) {
        final StringBuilder sb = new StringBuilder("{");
        for (T t : rdd.collect()) {
            sb.append(t).append(",");
        }
        if (sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 1); // 删除最后一个逗号
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * 将JavaPairRDD转换为字符串表示
     *
     * 用于调试和日志输出,格式为{(key1,value1),(key2,value2),...}
     *
     * @param rdd 输入的PairRDD
     * @return PairRDD内容的字符串表示
     */
    <K, V> String dump(JavaPairRDD<K, V> rdd) {
        final StringBuilder sb = new StringBuilder("{");
        for (Tuple2<K, V> pair : rdd.collect()) {
            sb.append("(").append(pair._1()).append(",").append(pair._2()).append(")").append(",");