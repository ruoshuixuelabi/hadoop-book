/**
 * 文件：ResilientActiveKeyValueStore.java
 * 路径：src/main/java/ResilientActiveKeyValueStore.java
 * 描述：可恢复的活跃键值存储类
 * <p>
 * 功能说明：
 * 继承自 ConnectionWatcher，提供具有重试机制的 ZooKeeper 读写操作。
 * 当遇到临时性错误（如连接中断）时，会自动重试指定次数。
 * <p>
 * 主要改进：
 * - 写入操作包含存在性检查和版本控制
 * - 失败时自动重试，最多 5 次
 * - 区分会话过期错误和其他错误
 * <p>
 * @see ConnectionWatcher
 */
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * 可恢复的活跃键值存储类
 * <p>
 * 继承自 ConnectionWatcher，提供具有重试机制的 ZooKeeper 读写操作。
 * 当遇到临时性错误（如连接中断）时，会自动重试指定次数。
 * <p>
 * 主要改进：
 * - 写入操作包含存在性检查和版本控制
 * - 失败时自动重试，最多 5 次
 * - 区分会话过期错误和其他错误
 */
public class ResilientActiveKeyValueStore extends ConnectionWatcher {

    /**
     * UTF-8 字符集编码<br>
     * 用于将字符串与字节数组之间进行转换
     */
    private static final Charset CHARSET = Charset.forName("UTF-8");

    /**
     * 最大重试次数<br>
     * 当遇到临时性错误时，最大重试次数
     */
    private static final int MAX_RETRIES = 5;

    /**
     * 重试间隔时间（秒）<br>
     * 每次重试之间的等待时间
     */
    private static final int RETRY_PERIOD_SECONDS = 10;

    /**
     * 写入数据到指定路径（带重试机制）<br>
     * <p>
     * 如果节点不存在则创建，存在则更新。<br>
     * 使用版本号进行乐观锁控制，确保并发更新的正确性。<br>
     * 当遇到临时性错误时自动重试，直到成功或达到最大重试次数。
     * <p>
     * @param path 节点路径
     * @param value 要写入的数据值
     * @throws InterruptedException 如果操作被中断
     * @throws KeeperException 如果 ZooKeeper 操作失败（达到最大重试次数后仍失败）
     */
    public void write(String path, String value) throws InterruptedException,
            KeeperException {
        // 重试计数器
        int retries = 0;
        // 循环直到成功或达到最大重试次数
        while (true) {
            try {
                // 检查节点是否存在
                Stat stat = zk.exists(path, false);
                if (stat == null) {
                    // 节点不存在，创建新节点
                    zk.create(path, value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                } else {
                    // 节点存在，使用当前版本号更新数据
                    // 版本号匹配确保不会覆盖其他客户端的更新
                    zk.setData(path, value.getBytes(CHARSET), stat.getVersion());
                }
                // 写入成功，返回
                return;
            } catch (KeeperException.SessionExpiredException e) {
                // 会话已过期，无法恢复，重新抛出异常
                // 会话过期是比较严重的问题，不应该重试
                throw e;
            } catch (KeeperException e) {
                // 其他 ZooKeeper 错误，检查重试次数
                if (retries++ == MAX_RETRIES) {
                    // 达到最大重试次数，抛出异常
                    throw e;
                }
                // 睡眠后重试
                // 使用 TimeUnit 而不是 Thread.sleep() 以便阅读
                TimeUnit.SECONDS.sleep(RETRY_PERIOD_SECONDS);
            }
        }
    }

    /**
     * 读取指定路径的数据<br>
     * <p>
     * 设置 watcher 用于监视数据变化。
     * <p>
     * @param path 节点路径
     * @param watcher 用于监视数据变化的 watcher，传入 null 表示不监视
     * @return 节点的数据内容
     * @throws InterruptedException 如果操作被中断
     * @throws KeeperException 如果 ZooKeeper 操作失败
     */
    public String read(String path, Watcher watcher) throws InterruptedException,
            KeeperException {
        // 读取数据并注册 watcher 用于监视变化
        byte[] data = zk.getData(path, watcher, null/*stat*/);
        return new String(data, CHARSET);
    }
}