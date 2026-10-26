import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.*;

/**
 * 在 MapFile 中按气温查找同一键的所有记录
 *
 * <p>与只查一条的版本相比，本例演示了 MapFile 的另一项能力：
 * 同一个键可以对应多条记录，定位到第一条之后可以顺着往下读
 */
public class LookupRecordsByTemperature extends Configured implements Tool {

  /**
   * 查找并打印同一个键的所有记录
   *
   * @param args 依次为 MapFile 路径与要查找的气温
   * @return 0 表示查到了，-1 表示没找到或参数错误
   * @throws Exception 读写失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 需要一个路径加一个键
    if (args.length != 2) {
      // 打印用法提示
      JobBuilder.printUsage(this, "<path> <key>");
      // 参数错误，返回 -1
      return -1;
    }
    // MapFile 的路径
    Path path = new Path(args[0]);
    // 要查找的键，也就是气温
    IntWritable key = new IntWritable(Integer.parseInt(args[1]));
    
    // 打开 MapFile 的全部分片
    Reader[] readers = MapFileOutputFormat.getReaders(path, getConf());
    // 必须用与写出时相同的分区器，才能算出键落在哪个分片
    Partitioner<IntWritable, Text> partitioner =
      new HashPartitioner<IntWritable, Text>();
    // 用来接收查到的值
    Text val = new Text();
    
    // 直接算出键所在的分片，并取出它的读取器
    Reader reader = readers[partitioner.getPartition(key, val, readers.length)];
    // 在这个分片里查找这个键
    Writable entry = reader.get(key, val);
    // 没查到
    if (entry == null) {
      // 报告键不存在
      System.err.println("Key not found: " + key);
      // 没找到，返回 -1
      return -1;
    }
    // 用解析器从记录里取出站号与年份
    NcdcRecordParser parser = new NcdcRecordParser();
    // 用来接收下一条记录的键
    IntWritable nextKey = new IntWritable();
    // 循环读取同一个键对应的所有记录
    do {
      // 解析当前这条记录
      parser.parse(val.toString());
      // 打印「站号\t年份」
      System.out.printf("%s\t%s\n", parser.getStationId(), parser.getYear());
    // 只要键没变就继续读：同一个键可能对应多条记录
    } while(reader.next(nextKey, val) && key.equals(nextKey));
    // 查询成功
    return 0;
  }
  
  /**
   * 程序入口
   *
   * @param args 命令行参数
   * @throws Exception 查询失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会解析通用选项并调用 run
    int exitCode = ToolRunner.run(new LookupRecordsByTemperature(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
