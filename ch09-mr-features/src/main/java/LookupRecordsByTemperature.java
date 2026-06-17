import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.*;

/**
 * 根据温度键查找所有匹配气象记录的程序.
 *
 * 本程序演示如何从MapFile中查找键对应的一条记录并遍历同键所有记录:
 *
 * 1. 功能:
 *    - 输入:MapFile路径和温度键
 *    - 输出:该温度对应的所有气象记录(可能有多条)
 *    - 当有多条记录温度相同时,全部列出
 *
 * 2. 查找流程:
 *    - 确定键所在的Reader分区
 *    - 获取该键对应的值
 *    - 使用next方法遍历同键的后续记录
 *
 * 3. 与LookupRecordByTemperature的区别:
 *    - LookupRecordByTemperature:精确匹配,只返回一条
 *    - 本程序:范围匹配,返回所有温度相同的记录
 *
 * 4. 迭代查找原理:
 *    - reader.get(key)获取第一条匹配记录
 *    - reader.next()继续向后遍历
 *    - 比较键是否仍然相等,决定是否继续
 *
 * @see MapFileOutputFormat
 * @see LookupRecordByTemperature
 */
public class LookupRecordsByTemperature extends Configured implements Tool {

    /**
     * 运行查找程序,获取温度键对应的所有记录.
     *
     * @param args 命令行参数:MapFile路径 温度键
     * @return 成功返回0,未找到或错误返回-1
     * @throws Exception 执行异常
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            JobBuilder.printUsage(this, "<path> <key>");
            return -1;
        }
        Path path = new Path(args[0]);
        IntWritable key = new IntWritable(Integer.parseInt(args[1]));

        // 获取所有Reader,确定键所在的分区Reader
        Reader[] readers = MapFileOutputFormat.getReaders(path, getConf());
        Partitioner<IntWritable, Text> partitioner =
            new HashPartitioner<IntWritable, Text>();
        Text val = new Text();

        // 获取键所在的Reader
        Reader reader = readers[partitioner.getPartition(key, val, readers.length)];
        // 获取键对应的值(第一条匹配的记录)
        Writable entry = reader.get(key, val);
        if (entry == null) {
            System.err.println("Key not found: " + key);
            return -1;
        }
        // 解析并打印第一条记录
        NcdcRecordParser parser = new NcdcRecordParser();
        IntWritable nextKey = new IntWritable();
        do {
            parser.parse(val.toString());
            System.out.printf("%s\t%s\n", parser.getStationId(), parser.getYear());
            // 使用next方法获取下一条记录,继续遍历直到键不匹配
        } while(reader.next(nextKey, val) && key.equals(nextKey));
        return 0;
    }

    /**
     * 程序入口.
     *
     * @param args 命令行参数:MapFile路径 温度键
     * @throws Exception 运行异常
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LookupRecordsByTemperature(), args);
        System.exit(exitCode);
    }
}