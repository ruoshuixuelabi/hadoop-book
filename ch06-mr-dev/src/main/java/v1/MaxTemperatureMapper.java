package v1;
// cc MaxTemperatureMapperV1 First version of a Mapper that passes MaxTemperatureMapperTest
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
//vv MaxTemperatureMapperV1
/**
 * 最初版的 Mapper：直接从记录文本里截出年份与气温
 *
 * <p>以 NCDC 气象记录为例，每行是定长文本：
 * 第 15~19 个字符是年份，第 87~92 个字符是气温
 * <p>这一版只求「能跑通」，还不做任何数据校验——
 * 后面的 v2、v3、v4 会逐个补上
 */
public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  
  /**
   * 从一行记录中解析出年份和气温并写出去
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
    
    // Text 转成 String，后面就能用 substring 截取
    String line = value.toString();
    // 年份固定在第 15~19 位（下标从 0 起，即第 16~19 个字符）
    String year = line.substring(15, 19);
    // 气温占 5 个字符。这里没做校验：温度为 9999（缺失）或带正号时都会出错，
    // 正是 v2 要修的问题
    int airTemperature = Integer.parseInt(line.substring(87, 92));
    // 以年份为键，这样 Reduce 端能把同一年份的记录聚到一起
    context.write(new Text(year), new IntWritable(airTemperature));
  }
}
//^^ MaxTemperatureMapperV1
