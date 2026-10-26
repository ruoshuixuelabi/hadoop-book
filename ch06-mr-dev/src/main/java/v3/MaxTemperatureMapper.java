// == MaxTemperatureMapperV3
package v3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import v2.NcdcRecordParser;

//vv MaxTemperatureMapperV3
/**
 * 第三版 Mapper：在 v2 之上增加「异常高温告警」
 *
 * <p>气温超过 1000 度显然不合常理，说明记录可能损坏
 * <p>v3 的做法是：照常输出（不丢数据），
 * 同时打一条日志、并把计数记进 Counter，
 * 这样跑完作业后既能在日志里看到细节，也能在 Web UI 上看到总数
 */
public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  // 计数器分组：按「类别」统计异常记录的数量，
  // 作业结束后可以在 Web UI 的 Counters 面板看到每一类的计数
  /*[*/enum Temperature {
    OVER_100
  }/*]*/
  
  // 解析器复用一个实例即可，它自身不保存跨记录的状态
  private NcdcRecordParser parser = new NcdcRecordParser();

  /**
   * 解析记录并输出温度，同时统计异常高温
   *
   * @param key 输入的键（此处用不到）
   * @param value 输入的值，即一整行记录
   * @param context 上下文，用来写出结果
   * @throws IOException 写出失败
   * @throws InterruptedException 任务被中断
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    // 先解析出年份、气温和质量标记
    parser.parse(value);
    // 只处理通过校验的记录
    if (parser.isValidTemperature()) {
      int airTemperature = parser.getAirTemperature();
      // 温度高得不合常理，多半是记录损坏。
      // 这里选择「记下来但不丢弃」：数据仍进入结果，
      // 同时通过日志和计数器把问题暴露出来
      /*[*/if (airTemperature > 1000) {
        System.err.println("Temperature over 100 degrees for input: " + value);
        context.setStatus("Detected possibly corrupt record: see logs.");
        context.getCounter(Temperature.OVER_100).increment(1);
      }/*]*/
      // 照常输出：可疑的数据也保留，只是不被静默丢掉
      context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
    }
  }
}
//^^ MaxTemperatureMapperV3

