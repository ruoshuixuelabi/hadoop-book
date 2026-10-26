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
 * Spark 最简单的入门测试
 *
 * <p>用一份很小的 {@code fruit.txt} 走一遍 Spark 的读写流程：
 * 读进来、缓存、过滤、计数
 * <p>这个类要实现 {@code Serializable}，
 * 因为里面定义的 {@code Function} 会被 Spark 序列化后分发到各个任务里
 */
public class SimpleTest implements Serializable { // needs to be serializable for the Fn

  /**
   * 临时目录规则
   *
   * <p>JUnit 会在每个测试方法前后自动创建和删除这个临时目录，
   * 用来放测试期间生成的输入、输出文件
   * <p>{@code transient} 是必须的：
   * 这个字段不属于测试逻辑，不该被序列化
   */
  @Rule
  public transient TemporaryFolder tmpDir = new TemporaryFolder();

  /**
   * 跑一遍最基本的 Spark 操作
   *
   * @throws IOException 复制样例文件失败时抛出
   */
  @Test
  public void test() throws IOException {
    // 在临时目录里造一个输入文件
    File inputFile = tmpDir.newFile("input");
    // 把 classpath 里的样例数据复制过来
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("fruit.txt")),
        inputFile);
    // Spark 的配置对象
    SparkConf conf = new SparkConf();
    // 建立本机模式的 Spark 上下文
    JavaSparkContext sc = new JavaSparkContext("local", "Simple App", conf);
    // 读入文本文件
    // cache() 把结果留在内存里，后面用两次以上才划算
    JavaRDD<String> a = sc.textFile(inputFile.getPath()).cache();

    // 先把内容打印出来，方便肉眼确认
    dump(a);

    // 挑出长度为偶数的那些行
    long numEven = a.filter(new Function<String, Boolean>() {
      /**
       * 判断一行要不要留下
       *
       * @param input 一行文本
       * @return 长度为偶数返回 {@code true}
       */
      public Boolean call(String input) {
        // 长度是偶数就留下
        return input.length() % 2 == 0; // even
      }
    }).count();

    // 断言偶数长度的行正好有 2 行
    assertEquals(2, numEven);

    // 关闭 Spark 上下文，释放资源
    sc.stop();
  }

  /**
   * 把 RDD 的内容拼成一个可读的字符串
   *
   * @param rdd 要打印的 RDD
   * @return 形如 {@code {a,b,c}} 的字符串
   */
  <T> String dump(JavaRDD<T> rdd) {
    // 先放一个左花括号
    final StringBuilder sb = new StringBuilder("{");
    // collect 会把分布式数据全拉到本地
    for (T t : rdd.collect()) {
      // 每个元素后面跟一个逗号
      sb.append(t).append(",");
    }
    // 长度大于 1 说明不是只有那个左花括号
    if (sb.length() > 1) {
      // 删掉最后那个多余的逗号
      sb.deleteCharAt(sb.length() - 1);
    }
    // 补上右花括号
    sb.append("}");
    // 返回拼好的字符串
    return sb.toString();
  }

  /**
   * 把键值对 RDD 的内容拼成一个可读的字符串
   *
   * @param rdd 要打印的键值对 RDD
   * @return 形如 {@code {(k1,v1),(k2,v2)}} 的字符串
   */
  <K, V> String dump(JavaPairRDD<K, V> rdd) {
    // 先放一个左花括号
    final StringBuilder sb = new StringBuilder("{");
    // collect 会把分布式数据全拉到本地
    for (Tuple2<K, V> pair : rdd.collect()) {
      // 每个键值对包成小括号，后面跟一个逗号
      sb.append("(").append(pair._1()).append(",").append(pair._2()).append(")").append(",");
    }
    // 长度大于 1 说明不是只有那个左花括号
    if (sb.length() > 1) {
      // 删掉最后那个多余的逗号
      sb.deleteCharAt(sb.length() - 1);
    }
    // 补上右花括号
    sb.append("}");
    // 返回拼好的字符串
    return sb.toString();
  }
}
