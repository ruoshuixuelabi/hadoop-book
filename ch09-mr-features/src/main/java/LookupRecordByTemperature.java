import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.*;

/**
 * 在 MapFile 中按气温查找一条记录
 *
 * <p>MapFile 是 Hadoop 的一种有序文件格式：
 * 它把数据按键排序后分片存放，并额外维护一份索引，
 * 因此既可以顺序遍历，也可以按键随机查找
 * <p>本例演示随机查找：先算出键落在哪个分片，
 * 再到那个分片里把记录读出来
 */
public class LookupRecordByTemperature extends Configured implements Tool {
  
  /**
   * 查找并打印一条记录
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
    // 直接调用框架的查找方法，它会定位分片并把记录读出来
    Writable entry =
      MapFileOutputFormat.getEntry(readers, partitioner, key, val);
    // 没查到
    if (entry == null) {
      // 报告键不存在
      System.err.println("Key not found: " + key);
      // 没找到，返回 -1
      return -1;
    }
    // 用解析器从记录里取出站号与年份
    NcdcRecordParser parser = new NcdcRecordParser();
    // 解析这条记录
    parser.parse(val.toString());
    // 打印「站号\t年份」
    System.out.printf("%s\t%s\n", parser.getStationId(), parser.getYear());
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
    int exitCode = ToolRunner.run(new LookupRecordByTemperature(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
