// cc OldMaxTemperature Application to find the maximum temperature, using the old MapReduce API

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

// vv OldMaxTemperature
/**
 * OldMaxTemperature - 使用旧版MapReduce API的示例程序
 *
 * <p>功能说明：</p>
 * 此程序展示了如何使用Hadoop旧版API（org.apache.hadoop.mapred）查找最高温度。
 * 旧版API在Hadoop 0.x时代使用，现已被新版API（org.apache.hadoop.mapreduce）取代。
 *
 * <p>新旧API的主要区别：</p>
 * <ul>
 *   <li>包名：旧版使用mapred包，新版使用mapreduce包</li>
 *   <li>类实现：旧版使用接口+继承，新版使用泛型+继承</li>
 *   <li>输出收集：旧版使用OutputCollector，新版使用Context</li>
 *   <li>配置方式：旧版使用JobConf，新版使用Job</li>
 * </ul>
 *
 * <p>注意：</p>
 * 此程序仅用于演示旧API的使用方式，实际开发中应使用新版API。
 *
 * @see MaxTemperature 新版API实现
 */
public class OldMaxTemperature {

    /**
     * OldMaxTemperatureMapper - 旧版API的Mapper实现
     *
     * <p>与新版Mapper的区别：</p>
     * <ul>
     *   <li>继承MapReduceBase基类</li>
     *   <li>实现Mapper接口并指定泛型类型参数</li>
     *   <li>使用OutputCollector收集输出</li>
     *   <li>使用Reporter报告进度</li>
     * </ul>
     */
    static class OldMaxTemperatureMapper /*[*/ extends MapReduceBase/*]*/
            /*[*/ implements Mapper/*]*/<LongWritable, Text, Text, IntWritable> {

        // 缺失值标记：9999表示数据缺失
        private static final int MISSING = 9999;

        /**
         * map方法 - 解析气象记录
         *
         * @param key       行偏移量
         * @param value     一行气象数据
         * @param output    输出收集器
         * @param reporter  进度 reporter
         */
        @Override
        public void map(LongWritable key, Text value,
                /*[*/OutputCollector<Text, IntWritable> output, Reporter reporter/*]*/)
                throws IOException {

            // 将行数据转换为字符串
            String line = value.toString();
            // 从固定位置提取年份（位置15-18）
            String year = line.substring(15, 19);

            int airTemperature;
            // 处理带前导'+'号的情况
            // Integer.parseInt无法解析"+0001"这样的格式
            if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
                // '+'号时从位置88开始读取4个字符
                airTemperature = Integer.parseInt(line.substring(88, 92));
            }
            else {
                // 普通情况从位置87开始读取5个字符
                airTemperature = Integer.parseInt(line.substring(87, 92));
            }

            // 提取数据质量标记（位置92）
            String quality = line.substring(92, 93);

            // 数据过滤：温度不为缺失值 且 质量标记可接受
            if (airTemperature != MISSING && quality.matches("[01459]")) {
                // 输出(年份, 温度)键值对
                /*[*/
                output.collect/*]*/(new Text(year), new IntWritable(airTemperature));
            }
        }
    }

    /**
     * OldMaxTemperatureReducer - 旧版API的Reducer实现
     *
     * <p>与新版Reducer的区别：</p>
     * <ul>
     *   <li>继承MapReduceBase基类</li>
     *   <li>实现Reducer接口并指定泛型类型参数</li>
     *   <li>使用Iterator遍历值列表</li>
     *   <li>使用OutputCollector收集输出</li>
     * </ul>
     */
    static class OldMaxTemperatureReducer /*[*/ extends MapReduceBase/*]*/
            /*[*/ implements Reducer/*]*/<Text, IntWritable, Text, IntWritable> {

        /**
         * reduce方法 - 求最大值
         *
         * @param key     年份
         * @param values  温度值迭代器
         * @param output  输出收集器
         * @param reporter 进度 reporter
         */
        @Override
        public void reduce(Text key, /*[*/Iterator/*]*/<IntWritable> values,
                /*[*/OutputCollector<Text, IntWritable> output, Reporter reporter/*]*/)
                throws IOException {

            // 初始化为最小整数值
            int maxValue = Integer.MIN_VALUE;

            // 遍历所有温度值找出最大值
            while (/*[*/values.hasNext()/*]*/) {
                maxValue = Math.max(maxValue, /*[*/values.next().get()/*]*/);
            }

            // 输出结果
            /*[*/
            output.collect/*]*/(key, new IntWritable(maxValue));
        }
    }

    /**
     * main方法 - 作业入口点
     *
     * <p>使用旧版API配置和提交作业：</p>
     * <ul>
     *   <li>创建JobConf配置对象</li>
     *   <li>设置各种作业参数</li>
     *   <li>调用JobClient.runJob()提交作业</li>
     * </ul>
     *
     * @param args 命令行参数
     * @throws IOException 如果发生IO错误
     */
    public static void main(String[] args) throws IOException {
        // 验证参数
        if (args.length != 2) {
            System.err.println("Usage: OldMaxTemperature <input path> <output path>");
            System.exit(-1);
        }

        // 创建JobConf配置对象，指定作业的主类
        /*[*/
        JobConf conf = new JobConf(OldMaxTemperature.class);/*]*/
        // 设置作业名称
        /*[*/
        conf/*]*/.setJobName("Max temperature");

        // 设置输入路径
        FileInputFormat.addInputPath(/*[*/conf/*]*/, new Path(args[0]));
        // 设置输出路径
        FileOutputFormat.setOutputPath(/*[*/conf/*]*/, new Path(args[1]));

        // 设置Mapper类
        /*[*/
        conf/*]*/.setMapperClass(OldMaxTemperatureMapper.class);
        // 设置Reducer类
        /*[*/
        conf/*]*/.setReducerClass(OldMaxTemperatureReducer.class);

        // 设置输出键的类型
        /*[*/
        conf/*]*/.setOutputKeyClass(Text.class);
        // 设置输出值的类型
        /*[*/
        conf/*]*/.setOutputValueClass(IntWritable.class);

        // 提交作业并等待完成（旧版API的提交方式）
        /*[*/
        JobClient.runJob(conf);/*]*/
    }
}
// ^^ OldMaxTemperature
