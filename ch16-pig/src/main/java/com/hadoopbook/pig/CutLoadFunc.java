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
 * CutLoadFunc是一个Pig LoadFunc UDF，用于按列范围提取数据。
 *
 * <p>该类实现了Pig的LoadFunc接口，允许用户通过类似Unix cut命令的语法
 * 指定要提取的列范围。例如，对于CSV格式的数据，可以使用"1-3,5-10"
 * 来提取第1-3列和第5-10列。</p>
 *
 * <p>使用示例（Pig Latin）：
 * <pre>
 * raw = LOAD 'data.csv' USING com.hadoopbook.pig.CutLoadFunc('1-3,5-10')
 *      AS (name:chararray, age:int, address:chararray);
 * </pre>
 *
 * <p>该加载器的工作流程：
 * <ol>
 *   <li>使用TextInputFormat读取文本文件的每一行</li>
 *   <li>根据cutPattern解析出的Range列表提取各列子串</li>
 *   <li>将提取的子串包装为Tuple返回</li>
 * </ol>
 * </p>
 *
 * <p>注意：
 * <ul>
 *   <li>范围是1-based索引（即第一列索引为1）</li>
 *   <li>范围是闭区间[start, end]，包括两端点</li>
 *   <li>超出行长度的范围会被跳过并记录警告</li>
 * </ul>
 * </p>
 *
 * @see Range
 * @see LoadFunc
 */
public class CutLoadFunc extends LoadFunc {

    /** 日志记录器，用于输出警告信息 */
    private static final Log LOG = LogFactory.getLog(CutLoadFunc.class);

    /** 要提取的列范围列表 */
    private final List<Range> ranges;

    /** Tuple工厂，用于创建Tuple实例 */
    private final TupleFactory tupleFactory = TupleFactory.getInstance();

    /** RecordReader，用于读取Hadoop输入数据 */
    private RecordReader reader;

    /**
     * 构造方法，解析cut模式字符串。
     *
     * @param cutPattern 列范围模式，格式为"start-end[,start-end,...]"
     *                   例如："1-3,5-10"表示提取第1-3列和第5-10列
     * @see Range#parse(String)
     */
    public CutLoadFunc(String cutPattern) {
        ranges = Range.parse(cutPattern);
    }

    /**
     * 设置输入数据的位置路径。
     *
     * <p>此方法在Pig执行作业时被调用，设置MapReduce作业的输入路径。</p>
     *
     * @param location 输入文件路径（HDFS路径或本地路径）
     * @param job      MapReduce作业配置
     * @throws IOException 如果设置输入路径失败
     */
    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    /**
     * 获取输入数据格式。
     *
     * <p>返回TextInputFormat用于读取文本文件，每行作为一条记录。</p>
     *
     * @return TextInputFormat实例
     */
    @Override
    public InputFormat getInputFormat() {
        return new TextInputFormat();
    }

    /**
     * 准备开始读取数据。
     *
     * <p>在开始处理输入分片之前调用，传入RecordReader实例供后续使用。</p>
     *
     * @param reader RecordReader实例，用于读取键值对
     * @param split  PigSplit，表示输入分片
     */
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        this.reader = reader;
    }

    /**
     * 获取下一条记录（Tuple）。
     *
     * <p>此方法是LoadFunc的核心实现，负责：
     * <ol>
     *   <li>从RecordReader读取下一行文本</li>
     *   <li>根据ranges列表提取各列子串</li>
     *   <li>将子串包装为DataByteArray并创建Tuple返回</li>
     * </ol>
     * </p>
     *
     * <p>如果读到文件末尾（reader.nextKeyValue()返回false），则返回null，
     * 这告诉Pig已经没有任何数据了。</p>
     *
     * @return 包含提取列值的Tuple，读取完毕时返回null
     * @throws IOException 如果读取输入数据失败
     */
    @Override
    public Tuple getNext() throws IOException {
        try {
            // 读取下一条记录，如果已到文件末尾则返回null
            if (!reader.nextKeyValue()) {
                return null;
            }
            // 获取当前行的文本值
            Text value = (Text) reader.getCurrentValue();
            String line = value.toString();
            // 创建与范围数量相同大小的Tuple
            Tuple tuple = tupleFactory.newTuple(ranges.size());
            // 遍历每个Range，提取对应的子串
            for (int i = 0; i < ranges.size(); i++) {
                Range range = ranges.get(i);
                // 检查范围是否超出行长度，超出时记录警告并跳过
                if (range.getEnd() > line.length()) {
                    LOG.warn(String.format(
                            "Range end (%s) is longer than line length (%s)",
                            range.getEnd(), line.length()));
                    continue;
                }
                // 提取子串并包装为DataByteArray放入Tuple
                tuple.set(i, new DataByteArray(range.getSubstring(line)));
            }
            return tuple;
        }
        catch (InterruptedException e) {
            // 中断异常转换为ExecException，保持Pig的异常处理机制
            throw new ExecException(e);
        }
    }
}
// ^^ CutLoadFunc
