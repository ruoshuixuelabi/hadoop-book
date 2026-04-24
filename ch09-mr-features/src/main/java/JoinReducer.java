// cc JoinReducer Reducer:用于连接已标记的站点记录和气象记录

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// vv JoinReducer

/**
 * Reduce端连接的Reducer:执行站点名称与气象记录的关联.
 *
 * 功能说明:
 * - 接收同站点ID的所有记录(站点记录在前,气象记录在后)
 * - 第一个值是站点名称,后续值是该站点的气象记录
 * - 将站点名称与每条气象记录连接后输出
 *
 * 输入格式:
 * - 键:TextPair(站点ID, 标记)
 * - 值:[站点名称, 气象记录1, 气象记录2, ...]
 *
 * 输出格式:
 * - 键:站点名称
 * - 值:气象记录(包含温度等数据)
 *
 * 执行流程:
 * 1. 获取迭代器,第一个值是站点名称
 * 2. 遍历剩余的气象记录
 * 3. 将站点名称与每条记录用Tab连接后输出
 *
 * 注意:
 * - 站点名称只读取一次,与每条气象记录配对输出
 * - 输出键是站点名称,而非站点ID,实现名称替换
 *
 * @see JoinRecordWithStationName
 * @see JoinRecordMapper
 * @see JoinStationMapper
 */
public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

    /**
     * Reduce方法:连接站点名称和气象记录.
     *
     * @param key     TextPair键(站点ID, 标记)
     * @param values  同站点所有记录,第一个是站点名称,后续是气象记录
     * @param context MapReduce上下文
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        // 获取第一个值:站点名称
        Text stationName = new Text(iter.next());
        // 遍历后续的气象记录,与站点名称连接后输出
        while (iter.hasNext()) {
            Text record = iter.next();
            // 格式:站点名称 + Tab + 气象记录
            Text outValue = new Text(stationName.toString() + "\t" + record.toString());
            // 输出站点名称作为键,连接后的记录作为值
            context.write(key.getFirst(), outValue);
        }
    }
}
// ^^ JoinReducer