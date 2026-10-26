package oldapi;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * 给站点记录打标记的 Mapper，用于 Reducer 端的连接（v1 版）
 *
 * <p>站点元数据文件里每行包含站号、站名等信息，
 * 本例把站号与站名取出来，按「站号 + 标记 0」输出
 * <p>标记 0 与天气记录的标记 1 一起，
 * 使同一站点的两类记录在 Reducer 端汇合到同一个分组里
 */
public class JoinStationMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, TextPair, Text> {
  // 站点元数据解析器实例
  private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

  /**
   * 解析站点记录并按「站号 + 标记」输出
   *
   * @param key 输入的键（本例用不到）
   * @param value 输入的值，即一行站点记录
   * @param output 结果收集器
   * @param reporter 状态报告器
   * @throws IOException 写出失败
   */
  public void map(LongWritable key, Text value,
      OutputCollector<TextPair, Text> output, Reporter reporter)
      throws IOException {

    // 只有能解析出站号与站名的记录才参与连接
    if (parser.parse(value)) {
      // 键的第二段是标记 0，表示这条是站点信息
      output.collect(new TextPair(parser.getStationId(), "0"),
          new Text(parser.getStationName()));
    }
  }
}
