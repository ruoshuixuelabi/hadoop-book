// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MaxTemperatureMapper - 气象数据温度提取Mapper
 *
 * <p>功能说明：</p>
 * 此Mapper负责解析气象数据记录，提取年份和温度信息，并以键值对形式输出。
 *
 * <p>输入格式（气象数据固定宽度格式）：</p>
 * <pre>
 * 第15-18字符：年份（如1949）
 * 第87字符：温度符号（'+'表示正温度，'-'表示负温度或零下）
 * 第87-91字符：温度值（整数，单位为十分之一摄氏度）
 * 第92字符：数据质量标记（0=正常，1=可疑，4=异常等）
 * </pre>
 *
 * <p>输出：</p>
 * <ul>
 *   <li>Key: Text类型，年份字符串（如"1949"）</li>
 *   <li>Value: IntWritable类型，温度值</li>
 * </ul>
 *
 * <p>数据过滤规则：</p>
 * <ul>
 *   <li>温度不等于9999（9999表示数据缺失）</li>
 *   <li>质量标记匹配[01459]（表示数据质量可接受）</li>
 * </ul>
 *
 * @see MaxTemperatureReducer
 * @see MaxTemperature
 */
public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  // 缺失值标记：9999表示气象站未记录该数据
  private static final int MISSING = 9999;

  /**
   * map方法 - 解析并过滤气象记录
   *
   * <p>此方法被Hadoop为每条输入记录调用一次，执行以下步骤：</p>
   * <ol>
   *   <li>将输入行转换为字符串</li>
   *   <li>从固定位置提取年份</li>
   *   <li>从固定位置提取温度值（处理前导+号）</li>
   *   <li>提取数据质量标记</li>
   *   <li>根据温度和质量标记过滤无效数据</li>
   *   <li>输出有效的(年份, 温度)键值对</li>
   * </ol>
   *
   * @param key     输入数据的行偏移量（LongWritable类型）</li>
   * @param value   输入的一行气象数据（Text类型）</li>
   * @param context 用于输出键值对的Context对象</li>
   * @throws IOException 如果发生输入输出错误</li>
   * @throws InterruptedException 如果操作被中断</li>
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    // 将Text类型的行数据转换为String便于处理
    String line = value.toString();
    // 从位置15开始提取4个字符作为年份
    String year = line.substring(15, 19);

    int airTemperature;
    // 检查第87个字符是否为'+'号
    // Java的Integer.parseInt()不能正确解析带前导加号的值（如"+0001"）
    // 因此需要特殊处理：如果是'+'号，则从位置88开始读取
    if (line.charAt(87) == '+') {
      // '+'号情况下，温度值从位置88开始，读取4个字符
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      // 普通情况下，温度值从位置87开始，读取5个字符（包含负号）
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }

    // 从位置92提取1个字符作为数据质量标记
    String quality = line.substring(92, 93);

    // 数据质量过滤：温度不为缺失值 且 质量标记为可接受的值
    // 质量标记含义：0=正常, 1=可疑, 4=异常, 5=保留, 9=保留
    // 只有质量标记为0、1、4、5、9时才认为数据可靠
    if (airTemperature != MISSING && quality.matches("[01459]")) {
      // 输出(年份, 温度)键值对
      context.write(new Text(year), new IntWritable(airTemperature));
    }
  }
}
// ^^ MaxTemperatureMapper
