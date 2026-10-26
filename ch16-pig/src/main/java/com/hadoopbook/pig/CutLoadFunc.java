//cc CutLoadFunc A LoadFunc UDF to load tuple fields as column ranges
package com.hadoopbook.pig;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

// vv CutLoadFunc
/**
 * 自定义 LoadFunc：按「列区间」切分文本行
 *
 * <p>Pig 自带的 {@code PigStorage} 是按分隔符切列的，
 * 而这份 NCDC 数据是定长格式的，
 * 所以这里自己实现一个 {@code LoadFunc}，
 * 按照调用者给定的区间（如 {@code "1-3,5-10"}）逐段切出字段
 * <p>区间从 1 开始计数，和 {@code cut} 命令的用法一致
 */
public class CutLoadFunc extends LoadFunc {

  // 日志：区间越界这类情况要记一笔
  private static final Log LOG = LogFactory.getLog(CutLoadFunc.class);

  // 要切出来的列区间，由构造参数解析而来
  private final List<Range> ranges;
  // Pig 的元组工厂，用来造出每一行的元组
  private final TupleFactory tupleFactory = TupleFactory.getInstance();
  // Pig 交给我们的记录读取器，真正取数据靠它
  private RecordReader reader;

  /**
   * 构造一个按给定区间切列的 LoadFunc
   *
   * @param cutPattern 切分模式，形式为 {@code "起-止,起-止"}，
   *        位置从 1 开始
   */
  public CutLoadFunc(String cutPattern) {
    // 把切分模式解析成一组区间
    ranges = Range.parse(cutPattern);
  }
  
  /**
   * 告诉 Pig 数据在哪里
   *
   * @param location 输入路径
   * @param job 作业对象
   * @throws IOException 设置输入路径失败
   */
  @Override
  public void setLocation(String location, Job job)
      throws IOException {
    // Pig 的 LoadFunc 就是通过设置输入路径来定位数据的
    FileInputFormat.setInputPaths(job, location);
  }
  
  /**
   * 声明用哪种输入格式读取数据
   *
   * @return 纯文本输入格式
   */
  @Override
  public InputFormat getInputFormat() {
    // 定长文本，按行读进来
    return new TextInputFormat();
  }
  
  /**
   * 正式读数据之前，把记录读取器保存下来
   *
   * @param reader Pig 传进来的记录读取器
   * @param split 本任务负责的数据分片
   */
  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) {
    // 记住它，之后 getNext 靠它取下一行
    this.reader = reader;
  }

  /**
   * 读出下一行，并按区间切成一个元组
   *
   * @return 切好的元组；数据读完时返回 {@code null}
   * @throws IOException 读取失败
   */
  @Override
  public Tuple getNext() throws IOException {
    try {
      // 没有下一行了，通知 Pig 输入结束
      if (!reader.nextKeyValue()) {
        return null;
      }
      // 取当前行的内容
      Text value = (Text) reader.getCurrentValue();
      // 转成字符串，方便按位置截取
      String line = value.toString();
      // 元组的字段个数等于区间的个数
      Tuple tuple = tupleFactory.newTuple(ranges.size());
      // 逐个区间切出字段
      for (int i = 0; i < ranges.size(); i++) {
        // 当前这个区间
        Range range = ranges.get(i);
        // 这一行比区间还短，跳过这个区间
        if (range.getEnd() > line.length()) {
          // 记一条警告，便于发现数据格式不对
          LOG.warn(String.format(
              "Range end (%s) is longer than line length (%s)",
              range.getEnd(), line.length()));
          // 这个字段留空，继续处理下一个区间
          continue;
        }
        // 把切出来的片段放进元组
        tuple.set(i, new DataByteArray(range.getSubstring(line)));
      }
      // 这一行处理完毕
      return tuple;
    } catch (InterruptedException e) {
      // Pig 用的是自己的异常类型，这里包一层再抛出去
      throw new ExecException(e);
    }
  }
}
// ^^ CutLoadFunc
