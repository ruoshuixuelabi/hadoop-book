package oldapi;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 旧API Mapper类: 从NCDC气象数据中解析并提取年份和气温信息.
 *
 * <p>该Mapper处理原始气象数据记录,每行数据格式固定(固定宽度格式):
 * <pre>
 * 0-1:   站点标识
 * 2-3:   气象要素标识
 * 4-9:   日期(YYYYMMDD)
 * ...
 * 15-18: 年份
 * ...
 * 87-91: 气温值(可能带符号)
 * 92:    质量码
 * </pre>
 *
 * <p>Map阶段的处理逻辑:
 * <ul>
 *   <li>输入: &lt;行偏移量, 原始数据行&gt;</li>
 *   <li>输出: &lt;年份字符串, 气温整数值&gt;</li>
 * </ul>
 *
 * <p>数据质量过滤: 仅处理质量码为0、1、4、5、9的记录,
 * 这些质量码表示温度观测数据是有效的.
 *
 * @see MaxTemperatureReducer
 */
public class MaxTemperatureMapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {

  /**
   * 缺失数据的标记值,NCDC数据中使用9999表示温度数据缺失.
   */
  private static final int MISSING = 9999;

  /**
   * Map任务的核心处理方法,解析每行气象数据.
   *
   * <p>处理流程:
   * <ol>
   *   <li>将输入的Text行转换为String格式</li>
   *   <li>从第15-18字符提取年份信息</li>
   *   <li>从第87-91字符提取气温值(处理正负号)</li>
   *   <li>从第92字符提取质量码</li>
   *   <li>验证数据有效性(非缺失值且质量码符合要求)</li>
   *   <li>输出&lt;年份, 气温&gt;键值对</li>
   * </ol>
   *
   * <p>质量码说明:
   * <ul>
   *   <li>0: 温度观测值,质量合格</li>
   *   <li>1: 温度观测值,质量可疑但可能正确</li>
   *   <li>4: 温度观测值,质量存疑</li>
   *   <li>5: 温度观测值,质量可疑</li>
   *   <li>9: 数据缺失或不可用</li>
   * </ul>
   * 正则表达式[01459]匹配上述有效质量码.
   *
   * @param key 行在文件中的偏移量(字节位置),Mapper不使用此值
   * @param value 一行原始气象数据记录
   * @param output 收集<年份, 气温>键值对的输出器
   * @param reporter 作业Reporter,用于报告进度和状态
   * @throws IOException 如果输出时发生IO错误
   */
  public void map(LongWritable key, Text value,
      OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {

    // 将Text类型的行数据转换为String便于处理
    String line = value.toString();
    // 从固定位置提取4位年份字符串(第15-18字符)
    String year = line.substring(15, 19);

    int airTemperature;
    // 处理气温值的符号位:
    // 如果第87个字符是'+',说明气温为正数,需从第88个字符开始读取
    // 因为Integer.parseInt()不支持前导加号
    if (line.charAt(87) == '+') {
      // 正温度: 从索引88开始读取4个字符(索引88-91)
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      // 负温度或零温度: 从索引87开始读取5个字符(含负号)
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }

    // 提取质量码(第92字符位置)
    String quality = line.substring(92, 93);

    // 数据有效性过滤:
    // 1. 气温不等于缺失值9999
    // 2. 质量码匹配有效值[01459]之一
    if (airTemperature != MISSING && quality.matches("[01459]")) {
      // 输出<年份, 气温>键值对供Reduce阶段处理
      output.collect(new Text(year), new IntWritable(airTemperature));
    }
  }
}