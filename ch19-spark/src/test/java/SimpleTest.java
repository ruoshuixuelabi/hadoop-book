/**
 * 文件：SimpleTest.java<br>
 * 路径：src/test/java/SimpleTest.java<br>
 * 描述：Spark基本操作和单元测试示例<br>
 *<br>
 * 功能说明：<br>
 * 本章演示了Apache Spark的基本操作和单元测试方法。<br>
 * 主要测试内容：<br>
 * - RDD创建和缓存<br>
 * - 过滤（filter）操作<br>
 * - 计数（count）操作<br>
 * - RDD到PairRDD的转换<br>
 * - dump工具方法用于调试输出<br>
 *<br>
 * 运行命令：<br>
 * mvn test -pl ch19-spark<br>
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
 * Spark简单测试类<br>
 * 演示Spark的基本API使用方法和单元测试技巧。<br>
 * 测试包括RDD创建、转换操作和行动操作。<br>
 *<br>
 * 注意：由于Spark的Function需要序列化，测试类实现了Serializable接口<br>
 */
public class SimpleTest implements Serializable { // 需要序列化，因为Spark的Function需要通过网络传输

    /**
     * 临时文件夹规则<br>
     * 测试结束后自动清理创建的临时文件<br>
     */
    @Rule
    public transient TemporaryFolder tmpDir = new TemporaryFolder();

    /**
     * 测试Spark基本功能<br>
     *<br>
     * 测试流程：<br>
     * 1. 创建临时输入文件<br>
     * 2. 创建Spark上下文<br>
     * 3. 读取文件创建RDD<br>
     * 4. 缓存RDD<br>
     * 5. 过滤偶数长度的字符串<br>
     * 6. 计数并验证结果<br>
     *<br>
     * @throws IOException 如果文件操作失败<br>
     */
    @Test
    public void test() throws IOException {
        // 创建临时输入文件
        File inputFile = tmpDir.newFile("input");

        // 从classpath复制fruit.txt到临时文件
        // Resources.getResource从classpath查找资源文件
        // Files.copy复制文件内容到目标文件
        Files.copy(Resources.newInputStreamSupplier(Resources.getResource("fruit.txt")),
                inputFile);

        // 创建Spark配置对象
        SparkConf conf = new SparkConf();

        // 创建Spark上下文
        // "local"模式表示本地单线程运行
        // "Simple App"是应用程序名称
        JavaSparkContext sc = new JavaSparkContext("local", "Simple App", conf);

        // 读取文本文件并缓存
        // cache()将RDD缓存在内存中，避免重复计算
        // textFile读取文件，每行作为一个元素
        JavaRDD<String> a = sc.textFile(inputFile.getPath()).cache();

        // 打印RDD内容（用于调试）
        dump(a);

        // 过滤偶数长度的字符串并计数
        // filter是transformation，count是action
        long numEven = a.filter(new Function<String, Boolean>() {
            public Boolean call(String input) {
                // input.length() % 2 == 0 判断是否为偶数长度
                return input.length() % 2 == 0; // 偶数长度
            }
        }).count();

        // 验证结果：fruit.txt中应该有2个偶数长度的单词
        // cherry(6个字符), apple(5个字符), banana(6个字符)
        // cherry和banana是偶数长度，apple是奇数长度
        assertEquals(2, numEven);

        // 停止Spark上下文，释放资源
        // 重要：每个SparkContext使用完毕后都应该停止
        sc.stop();
    }

    /**
     * 将JavaRDD转换为字符串表示<br>
     * 用于调试和日志输出，格式为{element1,element2,...}<br>
     *<br>
     * @param rdd 输入的RDD<br>
     * @return RDD内容的字符串表示<br>
     */
    <T> String dump(JavaRDD<T> rdd) {
        // StringBuilder用于高效拼接字符串
        final StringBuilder sb = new StringBuilder("{");
        // collect()是action，将RDD所有元素收集到driver端
        for (T t : rdd.collect()) {
            sb.append(t).append(",");
        }
        if (sb.length() > 1) {
            // 删除最后一个多余的逗号
            sb.deleteCharAt(sb.length() - 1); // 删除最后一个逗号
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * 将JavaPairRDD转换为字符串表示<br>
     * 用于调试和日志输出，格式为{(key1,value1),(key2,value2),...}<br>
     *<br>
     * @param rdd 输入的PairRDD<br>
     * @return PairRDD内容的字符串表示<br>
     */
    <K, V> String dump(JavaPairRDD<K, V> rdd) {
        final StringBuilder sb = new StringBuilder("{");
        // 遍历PairRDD中的每个(key, value)元组
        for (Tuple2<K, V> pair : rdd.collect()) {
            // Tuple2的_1()获取key，_2()获取value
            sb.append("(").append(pair._1()).append(",").append(pair._2()).append(")").append(",");
        }
        if (sb.length() > 1) {
            // 删除最后一个多余的逗号
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("}");
        return sb.toString();
    }
}
