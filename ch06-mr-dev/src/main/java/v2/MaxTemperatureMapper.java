package v2;

// cc MaxTemperatureMapperV2 A Mapper that uses a utility class to parse records

/**
 * 最高温度MapReduce Mapper（版本2）
 * <p>
 * 此版本使用NcdcRecordParser工具类来解析NCDC气象记录，相比直接解析更加健壮。
 * <p>
 * 功能说明：<br/>
 * - 输入：NCDC格式的原始气象数据记录<br/>
 * - 输出：(年份, 温度)键值对
 * <p>
 * Mapper的典型用途：<br/>
 * - 读取输入分片中的每条记录<br/>
 * - 解析记录提取有用字段<br/>
 * - 过滤无效数据（如缺失温度）<br/>
 * - 输出中间键值对供Reducer处理
 *
 * @see NcdcRecordParser
 * @see MaxTemperatureReducer
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import v2.NcdcRecordParser;

// vv MaxTemperatureMapperV2
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /*[*/private NcdcRecordParser parser = new NcdcRecordParser();/*]*/

    /**
     * 解析每条NCDC记录并输出有效的温度数据
     * <p>
     * 只有有效温度才输出<br/>
     * 输出：(年份, 温度)键值对
     *
     * @param key     输入分片的行偏移量
     * @param value   NCDC格式的原始记录
     * @param context MapReduce上下文，用于输出键值对
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*[*/
        parser.parse(value);/*]*/
        // 只有有效温度才输出
        if (/*[*/parser.isValidTemperature()/*]*/) {
            // 输出：(年份, 温度)键值对
            context.write(new Text(/*[*/parser.getYear()/*]*/), new IntWritable(/*[*/parser.getAirTemperature()/*]*/));
        }
    }
}
// ^^ MaxTemperatureMapperV2