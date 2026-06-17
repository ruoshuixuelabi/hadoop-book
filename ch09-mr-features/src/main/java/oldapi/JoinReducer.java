package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * Reduce端连接的Reducer:执行站点名称与气象记录的关联(旧API版本).
 *
 * 本程序是JoinReducer的旧API版本,
 * 使用org.apache.hadoop.mapred包中的旧接口.
 *
 * 功能说明:
 * - 接收同站点ID的所有记录(站点记录在前,气象记录在后)
 * - 第一个值是站点名称,后续值是该站点的气象记录
 * - 将站点名称与每条气象记录连接后输出
 *
 * @see JoinRecordWithStationName
 */
public class JoinReducer extends MapReduceBase implements
    Reducer<TextPair, Text, Text, Text> {

    /**
     * Reduce方法:连接站点名称和气象记录.
     *
     * @param key        TextPair键(站点ID, 标记)
     * @param values     同站点所有记录,第一个是站点名称,后续是气象记录
     * @param output     输出收集器
     * @param reporter   作业Reporter
     * @throws IOException IO异常
     */
    public void reduce(TextPair key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {

        // 获取第一个值:站点名称
        Text stationName = new Text(values.next());
        // 遍历后续的气象记录,与站点名称连接后输出
        while (values.hasNext()) {
            Text record = values.next();
            // 格式:站点名称 + Tab + 气象记录
            Text outValue = new Text(stationName.toString() + "\t" + record.toString());
            output.collect(key.getFirst(), outValue);
        }
    }
}