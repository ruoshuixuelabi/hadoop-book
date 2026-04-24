package oldapi;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.*;

/**
 * 根据温度键精确查找单条气象记录的程序(旧API版本).
 *
 * 本程序是LookupRecordByTemperature的旧API版本,
 * 使用org.apache.hadoop.mapred包中的旧接口.
 *
 * 功能:
 * 1. 输入:MapFile路径和温度键
 * 2. 输出:该温度对应的第一条气象记录
 * 3. 使用MapFileOutputFormat.getReaders获取Reader数组
 *
 * 与新API的区别:
 * - 使用FileSystem获取Path对应的文件系统
 * - 使用MapFileOutputFormat.getReaders(fs, path, conf)重载方法
 *
 * @see MapFileOutputFormat
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
        IntWritable key = new IntWritable(Integer.parseInt(args[1]));
        // 获取MapFile所在的文件系统
        FileSystem fs = path.getFileSystem(getConf());

        // 获取MapFile的所有Reader(对应各分区)
        Reader[] readers = /*[*/MapFileOutputFormat.getReaders(fs, path, getConf())/*]*/;
        // 创建HashPartitioner确定键所属的分区Reader
        Partitioner<IntWritable, Text> partitioner =
            new HashPartitioner<IntWritable, Text>();
        Text val = new Text();

        // 使用getEntry精确获取键对应的值
        Writable entry =
            /*[*/MapFileOutputFormat.getEntry(readers, partitioner, key, val)/*]*/;
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