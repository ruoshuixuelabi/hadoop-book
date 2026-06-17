//cc LoggingIdentityMapper An identity mapper that writes to standard output and also uses the Apache Commons Logging API

import java.io.IOException;
//vv LoggingIdentityMapper
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

public class LoggingIdentityMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    /**
     * 获取Logger实例
     * <p>
     * 使用 Commons Logging API，类名作为日志来源标识
     */
    private static final Log LOG = LogFactory.getLog(LoggingIdentityMapper.class);

    /**
     * Map函数：直接传递输入键值对到输出
     * <p>
     * 日志记录点：<br/>
     * 1. 使用System.out.println记录到标准输出文件<br/>
     * 2. 使用LOG.info记录到syslog文件<br/>
     * 3. 使用LOG.debug在DEBUG模式下记录详细信息
     *
     * @param key     输入键
     * @param value   输入值
     * @param context MapReduce上下文
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    @Override
    @SuppressWarnings("unchecked")
    public void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
        // Log to stdout file
        // 记录到stdout文件
        System.out.println("Map key: " + key);
        // Log to syslog file
        // 记录到syslog文件（INFO级别）
        LOG.info("Map key: " + key);
        // 仅在DEBUG启用时记录详细值
        if (LOG.isDebugEnabled()) {
            LOG.debug("Map value: " + value);
        }
        // 将键值对直接传递到输出（Identity Mapper行为）
        context.write((KEYOUT) key, (VALUEOUT) value);
    }
}
//^^ LoggingIdentityMapper