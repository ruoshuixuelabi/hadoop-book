package v4;
// cc MaxTemperatureMapperV4 Mapper for maximum temperature example

/**
 * 最高温度MapReduce Mapper（版本4）
 *
 * 此版本在v3基础上增强了错误处理能力，能够检测并统计格式错误的温度记录。
 *
 * 功能说明：
 * - 输入：NCDC格式的原始气象数据
 * - 输出：(年份, 温度)键值对，用于计算每年最高温度
 *
 * 与版本3的区别：
 * - 使用增强版NcdcRecordParser，可以检测格式错误的温度
 * - 通过Counter统计格式错误的记录数量
 *
 * 计数器说明：
 * - Temperature.MALFORMED: 统计格式错误的温度记录数
 *
 * @see NcdcRecordParser
 * @see MaxTemperatureReducer
 * @see <a href="https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/Counter.html">Counter</a>
 */
// vv MaxTemperatureMapperV4
public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  /**
   * 温度相关计数器枚举
   * 用于在MapReduce任务中追踪特定事件
   */
  enum Temperature {
    /**
     * 格式错误的温度记录计数器
     */
    MALFORMED
  }

  // 使用增强版解析器，支持检测格式错误
  private NcdcRecordParser parser = new NcdcRecordParser();

  /**
   * 解析NCDC记录并输出有效温度数据
   *
   * @param key     输入分片的行偏移量
   * @param value   NCDC格式的原始记录
   * @param context MapReduce上下文
   * @throws IOException          IO异常
   * @throws InterruptedException 中断异常
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    parser.parse(value);
    if (parser.isValidTemperature()) {
      // 输出有效的温度记录
      int airTemperature = parser.getAirTemperature();
      context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
    } else if (parser.isMalformedTemperature()) {
      // 输出格式错误记录信息到标准错误流
      System.err.println("Ignoring possibly corrupt input: " + value);
      // 增加格式错误计数器
      context.getCounter(Temperature.MALFORMED).increment(1);
    }
  }
}
// ^^ MaxTemperatureMapperV4