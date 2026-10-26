package v2;
// cc MaxTemperatureMapperV2 A Mapper that uses a utility class to parse records

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import v2.NcdcRecordParser;

// vv MaxTemperatureMapperV2
/**
 * 第二版 Mapper：把「解析与校验」外包给 {@link NcdcRecordParser}
 *
 * <p>v1 把解析逻辑直接写在 map 里，一旦要加校验规则就会越写越长
 * <p>这一版把解析抽成独立的类，Mapper 只负责「问它结果，再决定写不写」
 * <p>抽出来的另一个好处是：解析器可以被单元测试直接覆盖
 */
public class MaxTemperatureMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

  /**
   * 解析器实例
   *
   * <p>每个 Map 任务只有一份，不是每条记录一份——
   * {@code map} 方法可能被调用上百万次，实例应当复用
   */
  /*[*/private NcdcRecordParser parser = new NcdcRecordParser();/*]*/

  /**
   * 解析一条记录，有效才写出
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

    // 解析器把这一行拆成年份、气温、质量标记，并判断是否可用
    /*[*/parser.parse(value);/*]*/
    // 只输出通过校验的记录：温度缺失或质量位可疑的都跳过
    if (/*[*/parser.isValidTemperature()/*]*/) {
      context.write(new Text(/*[*/parser.getYear()/*]*/),
          new IntWritable(/*[*/parser.getAirTemperature()/*]*/));
    }
  }
}
// ^^ MaxTemperatureMapperV2
