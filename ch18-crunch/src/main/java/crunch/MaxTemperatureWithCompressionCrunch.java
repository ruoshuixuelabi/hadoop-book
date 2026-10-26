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
/**
 * 用 Crunch 求每年的最高气温，输出时用 gzip 压缩
 *
 * <p>这是第 12 章 {@code MaxTemperatureWithCompression} 的 Crunch 版本：
 * 计算过程和普通版本一样，
 * 区别是给管道配了一份开启了压缩的 {@code Configuration}
 * <p>压缩由 MapReduce 的输出格式完成，
 * 对 Crunch 来说是透明的：
 * 管道这边不用改任何代码
 */
public class MaxTemperatureWithCompressionCrunch {

  // NCDC 用 9999 表示温度缺失
  private static final int MISSING = 9999;

  /**
   * 作业入口
   *
   * @param args 两个参数：输入目录、输出目录
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // 参数必须是两个
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCompressionCrunch <input path> <output path>");
      // 直接退出
      System.exit(-1);
    }

    // 自己准备一份配置，用来打开输出压缩
    Configuration conf = new Configuration();
    // 打开输出压缩
    conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
    // 指定用 gzip 这个编解码器
    conf.setClass("mapreduce.output.fileoutputformat.compress.codec", GzipCodec.class, CompressionCodec.class);

    // 把这份配置交给管道
    Pipeline pipeline = new MRPipeline(MaxTemperatureWithCompressionCrunch.class, conf);
    // 读入文本文件
    PCollection<String> records = pipeline.readTextFile(args[0]);
    
    // 按年分组求最大值
    PTable<String, Integer> maxTemps = records
      .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()))
      .groupByKey()
      .combineValues(Aggregators.MAX_INTS());

    // 写出的文件会是 .gz
    pipeline.writeTextFile(maxTemps, args[1]);
    // 触发作业执行
    pipeline.run();
  }

  /**
   * 造一个把文本行变成「年 -> 气温」键值对的 {@code DoFn}
   *
   * <p>这里没有用 {@link NcdcRecordParser}，
   * 而是自己按固定位置切字段，
   * 为的是说明 Crunch 的 {@code DoFn} 也能像普通 Java 一样直接处理字符串
   *
   * @return 供 {@code parallelDo} 使用的处理函数
   */
  private static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
    return new DoFn<String, Pair<String, Integer>>() {
      /**
       * 处理一行 NCDC 记录
       *
       * @param input 一行文本
       * @param emitter 用来发出结果
       */
      @Override
      public void process(String input, Emitter<Pair<String, Integer>> emitter) {
        // 转成字符串才好按位置截取
        String line = input.toString();
        // 年份在第 15 到第 19 个字符
        String year = line.substring(15, 19);
        // 温度占 5 个字符，带正负号
        int airTemperature;
        // 带正号时要跳过正号本身
        // 上面那句英文是原作者留的提醒：Integer.parseInt 不接受开头的加号
        if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
          // 正数：从第 88 个字符开始取 4 位
          airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
          // 负数：连负号一起取 5 位
          airTemperature = Integer.parseInt(line.substring(87, 92));
        }
        // 质量码是第 93 个字符
        String quality = line.substring(92, 93);
        // 温度缺失或者质量码不合格的记录都要丢掉
        if (airTemperature != MISSING && quality.matches("[01459]")) {
          // 键是年份，值是气温
          emitter.emit(Pair.of(year, airTemperature));
        }
      }
    };
  }

}
