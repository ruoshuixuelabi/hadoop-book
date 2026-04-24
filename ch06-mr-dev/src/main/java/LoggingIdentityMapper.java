//cc LoggingIdentityMapper An identity mapper that writes to standard output and also uses the Apache Commons Logging API

/**
 * 日志记录Identity Mapper
 *
 * 这是一个身份映射器（Identity Mapper），直接将输入键值对传递到输出。
 * 同时演示两种日志记录方式的配置和使用。
 *
 * 日志记录方式说明：
 * 1. System.out.println() - 输出到stdout文件
 *    - 位置：标准输出文件
 *    - 用途：快速调试信息
 *
 * 2. Apache Commons Logging - 输出到syslog文件
 *    - 位置：syslog文件
 *    - 级别：INFO（基本信息）、DEBUG（详细信息）
 *    - 优势：可配置、标准化
 *
 * 设计目的：
 * - 演示MapReduce中如何正确使用日志记录
 * - 展示如何在不同日志级别间切换
 * - 帮助开发者调试Mapper逻辑
 *
 * 日志配置：
 * 需要在log4j.properties或log4j.xml中配置：
 * log4j.logger.org.apache.hadoop.mapreduce.Mapper=DEBUG
 *
 * @param <KEYIN>  输入键类型
 * @param <VALUEIN> 输入值类型
 * @param <KEYOUT> 输出键类型
 * @param <VALUEOUT> 输出值类型
 * @see org.apache.commons.logging.Log
 * @see org.apache.commons.logging.LogFactory
 */
//vv LoggingIdentityMapper
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

public class LoggingIdentityMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  /**
   * 获取Logger实例
   * 使用 Commons Logging API，类名作为日志来源标识
   */
  private static final Log LOG = LogFactory.getLog(LoggingIdentityMapper.class);

  /**
   * Map函数：直接传递输入键值对到输出
   *
   * 日志记录点：
   * 1. 使用System.out.println记录到标准输出文件
   * 2. 使用LOG.info记录到syslog文件
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
  public void map(KEYIN key, VALUEIN value, Context context)
          throws IOException, InterruptedException {
    // 记录到stdout文件
    System.out.println("Map key: " + key);

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