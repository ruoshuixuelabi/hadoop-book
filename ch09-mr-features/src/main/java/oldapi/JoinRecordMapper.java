package oldapi;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * 给天气记录打标记的 Mapper，用于 Reducer 端的连接（v1 版）
 *
 * <p>思路与新 API 版完全一样：
 * 把天气记录按「站号 + 标记 1」输出，
 * 与站点信息（标记 0）汇合到同一个分组里
 * <p>差别只在 v1 的接口约定：结果通过 {@code output.collect} 写出，
 * 而不是 v2 的 {@code context.write}
 */
public class JoinRecordMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, TextPair, Text> {
  // 解析器实例：一个 Map 任务只创建一次
  private NcdcRecordParser parser = new NcdcRecordParser();
  
  /**
   * 解析记录并按「站号 + 标记」输出
   *
   * @param key 输入的键（本例用不到）
   * @param value 输入的值，即一行 NCDC 记录
   * @param output 结果收集器
   * @param reporter 状态报告器
   * @throws IOException 写出失败
   */
  public void map(LongWritable key, Text value,
      OutputCollector<TextPair, Text> output, Reporter reporter)
      throws IOException {
    
    // 解析这一行记录，取出站号等字段
    parser.parse(value);
    // 键的第二段是标记 1，表示这条是天气记录
    output.collect(new TextPair(parser.getStationId(), "1"), value);
  }
}
