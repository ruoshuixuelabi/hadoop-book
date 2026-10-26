//cc LoggingIdentityMapper An identity mapper that writes to standard output and also uses the Apache Commons Logging API
import java.io.IOException;

//vv LoggingIdentityMapper
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 原样输出键值的 Mapper，同时演示两种日志写法
 *
 * <p>它不做任何加工，只把输入的 key/value 原样写出去，
 * 主要用途是观察日志在 Map 任务里的输出形式
 * <p>日志写了两种：{@code System.out} 直接打到标准输出，
 * 以及 Apache Commons Logging 的 {@code LOG}
 */
public class LoggingIdentityMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
  extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  
  // 静态字段：整个类共用一个 Log 实例，按类名可在日志配置里单独调级别
  private static final Log LOG = LogFactory.getLog(LoggingIdentityMapper.class);
  
  /**
   * 原样写出一条记录，并打出两种日志
   *
   * @param key 输入的键
   * @param value 输入的值
   * @param context 上下文，用来写出结果
   * @throws IOException 写出失败
   * @throws InterruptedException 任务被中断
   */
  @Override
  @SuppressWarnings("unchecked")
  public void map(KEYIN key, VALUEIN value, Context context)
      throws IOException, InterruptedException {
    // Log to stdout file
    // 直接写 stdout：绕过了日志框架，无法按级别过滤，
    // 输出量大时容易把磁盘刷满
    System.out.println("Map key: " + key);
    
    // Log to syslog file
    // 走日志框架：可以按级别控制，也便于集中收集
    LOG.info("Map key: " + key);
    // 先问一句再拼字符串，避免在关掉 debug 时白做一次字符串拼接
    if (LOG.isDebugEnabled()) {
      // 字符串拼接在 debug 关闭时是有开销的
      LOG.debug("Map value: " + value);
    }
    // 原样输出：key/value 的类型没变，所以可以直接强转
    context.write((KEYOUT) key, (VALUEOUT) value);
  }
}
//^^ LoggingIdentityMapper