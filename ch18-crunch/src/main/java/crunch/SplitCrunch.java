package crunch;

import java.io.Serializable;
import org.apache.crunch.FilterFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;

/**
 * 用 Crunch 把 NCDC 记录分成「好的」和「坏的」两堆
 *
 * <p>这是全书最早出现的 Crunch 例子：
 * 用 {@code filter} 加上一个 {@code FilterFn}，
 * 就能把一个 {@code PCollection} 拆成两个
 * <p>{@code FilterFn} 的 {@code accept} 方法返回 {@code true} 表示保留这条数据
 */
public class SplitCrunch implements Serializable {
  /**
   * 作业入口
   *
   * @param args 本例不用命令行参数，输入输出路径写死在代码里
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // 建立管道
    Pipeline pipeline = new MRPipeline(SplitCrunch.class);
    // 读入一小份带坏数据的样例文件
    PCollection<String> records = pipeline.readTextFile("input/ncdc/micro-tab/sample_corrupt.txt");

    // 第一个过滤器：留下气温合格的记录
    PCollection<String> goodRecords = records.filter(new FilterFn<String>() {
      // 解析器复用
      NcdcRecordParser parser = new NcdcRecordParser();
      /**
       * 判断一条记录要不要留下
       *
       * @param s 一行原始记录
       * @return 气温合格返回 {@code true}
       */
      @Override public boolean accept(String s) {
        // 拆成各个字段
        parser.parse(s);
        // 合格就留下
        return parser.isValidTemperature();
      }
    });

    // 第二个过滤器：留下气温不合格的记录
    PCollection<String> badRecords = records.filter(new FilterFn<String>() {
      // 解析器复用
      NcdcRecordParser parser = new NcdcRecordParser();
      /**
       * 判断一条记录要不要留下
       *
       * @param s 一行原始记录
       * @return 气温不合格返回 {@code true}
       */
      @Override public boolean accept(String s) {
        // 拆成各个字段
        parser.parse(s);
        // 不合格就留下
        return !parser.isValidTemperature();
      }
    });

    // 好数据写一份
    pipeline.writeTextFile(goodRecords, "output-good");
    // 坏数据写一份
    pipeline.writeTextFile(badRecords, "output-bad");
    // 触发作业执行
    pipeline.run();
  }
}
