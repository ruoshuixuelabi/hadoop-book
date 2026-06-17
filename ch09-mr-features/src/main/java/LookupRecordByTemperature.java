import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.*;

/**
 * 根据温度键精确查找单条气象记录的程序.
 *
 * 本程序演示如何从MapFile中按键精确查找记录:
 *
 * 1. 功能:
 *    - 输入:MapFile路径和温度键
 *    - 输出:该温度对应的第一条气象记录
 *    - 使用二分查找定位键值对
 *
 * 2. 查找流程:
 *    - 获取所有Reader(对应各分区)
 *    - 使用Partitioner确定键所在的Reader
 *    - 调用MapFileOutputFormat.getEntry获取精确值
 *
 * 3. 与LookupRecordsByTemperature的区别:
 *    - 本程序:精确匹配,返回一条记录
 *    - LookupRecordsByTemperature:范围匹配,返回多条记录
 *
 * 4. MapFile结构:
 *    - data文件:存储键值对,按键顺序排列
 *    - index文件:存储采样索引,加速查找
 *
 * @see MapFileOutputFormat
 * @see LookupRecordsByTemperature
 */
public class LookupRecordByTemperature extends Configured implements Tool {

    /**
     * 运行查找程序,根据温度键获取记录.
     *
     * @param args 命令行参数:MapFile路径 温度键
     * @return 找到返回0,未找到或错误返回-1
     * @throws Exception 执行异常
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            JobBuilder.printUsage(this, "<path> <key>");
            return -1;
        }
        Path path = new Path(args[0]);
        // 将命令行参数转换为IntWritable温度键
        IntWritable key = new IntWritable(Integer.parseInt(args[1]));

        // 获取MapFile的所有Reader(对应各分区)
        Reader[] readers = MapFileOutputFormat.getReaders(path, getConf());
        // 创建HashPartitioner确定键所属的分区Reader
        Partitioner<IntWritable, Text> partitioner =
            new HashPartitioner<IntWritable, Text>();
        Text val = new Text();

        // 计算键所在的Reader索引
        Reader reader = readers[partitioner.getPartition(key, val, readers.length)];
        // 使用getEntry精确获取键对应的值
        Writable entry =
            MapFileOutputFormat.getEntry(readers, partitioner, key, val);
        if (entry == null) {
            System.err.println("Key not found: " + key);
            return -1;
        }
        // 解析获取的记录,提取站点ID和年份
        NcdcRecordParser parser = new NcdcRecordParser();
        parser.parse(val.toString());
        System.out.printf("%s\t%s\n", parser.getStationId(), parser.getYear());
        return 0;
    }

    /**
     * 程序入口.
     *
     * @param args 命令行参数:MapFile路径 温度键
     * @throws Exception 运行异常
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LookupRecordByTemperature(), args);
        System.exit(exitCode);
    }
}