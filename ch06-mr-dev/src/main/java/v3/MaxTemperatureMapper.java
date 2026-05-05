// == MaxTemperatureMapperV3
package v3;

/**
 * 最高温度MapReduce Mapper（版本3）
 * <p>
 * 此版本增加了温度异常检测功能，使用计数器追踪超过100华氏度的记录。
 * <p>
 * 功能说明：<br/>
 * - 输入：NCDC格式的原始气象数据<br/>
 * - 处理：解析记录，检测异常高温<br/>
 * - 输出：(年份, 温度)键值对
 * <p>
 * 计数器功能说明：<br/>
 * 当检测到温度值超过1000（华氏度）时：<br/>
 * 1. 将错误信息输出到标准错误流<br/>
 * 2. 更新Mapper状态信息<br/>
 * 3. 增加OVER_100计数器
 * <p>
 * 设计目的：<br/>
 * 演示如何在MapReduce中使用Counters来检测和处理异常数据
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

//vv MaxTemperatureMapperV3
public class MaxTemperatureMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 温度相关计数器枚举
     * <p>
     * 用于追踪异常高温记录
     */
    /*[*/enum Temperature {
        /**
         * 超过100华氏度的温度计数器
         * <p>
         * 注意：Hadoop中温度值是原始值（如1000表示100.0度）
         */
        OVER_100
    }/*]*/

    // 使用v2版本的解析器
    private NcdcRecordParser parser = new NcdcRecordParser();

    /**
     * 解析每条记录，检测异常高温并输出有效数据
     * <p>
     * 使用v2版本的解析器解析记录<br/>
     * 检测异常高温：温度值超过1000（即100华氏度以上）<br/>
     * 输出有效的年份-温度键值对
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
            int airTemperature = parser.getAirTemperature();
            /*[*/
            // 检测异常高温：温度值超过1000（即100华氏度以上）
            if (airTemperature > 1000) {
                // 输出详细信息到标准错误流，供日志分析
                System.err.println("Temperature over 100 degrees for input: " + value);
                // 更新Mapper状态，帮助监控
                context.setStatus("Detected possibly corrupt record: see logs.");
                // 增加异常计数器
                context.getCounter(Temperature.OVER_100).increment(1);
            }/*]*/
            // 输出有效的年份-温度键值对
            context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
        }
    }
}
//^^ MaxTemperatureMapperV3