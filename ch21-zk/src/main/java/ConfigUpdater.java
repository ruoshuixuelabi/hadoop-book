//cc ConfigUpdater 一个在随机时间更新ZooKeeper中配置属性的应用程序

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

// vv ConfigUpdater

/**
 * 配置更新器程序
 * <p>
 * 该程序定期随机更新ZooKeeper中/config节点的数据。
 * 用于演示ZooKeeper的配置管理功能和watch机制。
 * <p>
 * 使用说明:<br/>
 * java ConfigUpdater <zookeeper地址>
 * <p>
 * 工作流程:<br/>
 * 1. 连接到ZooKeeper服务器<br/>
 * 2. 每隔1-10秒随机生成一个0-99的数值<br/>
 * 3. 将新值写入/config节点<br/>
 * 4. 重复执行
 *
 * @example
 * java ConfigUpdater localhost:2181
 * // 输出: Set /config to 42
 * //       Set /config to 17
 * //       ...
 *
 * @see ConfigWatcher
 */
public class ConfigUpdater {

    /** 配置节点路径 */
    public static final String PATH = "/config";

    /** 活跃的键值存储实例 */
    private ActiveKeyValueStore store;

    /** 随机数生成器 */
    private Random random = new Random();

    /**
     * 构造函数
     * <p/>
     * 创建ActiveKeyValueStore实例并连接到ZooKeeper服务器
     *
     * @param hosts ZooKeeper服务器地址
     * @throws IOException 如果发生I/O错误
     * @throws InterruptedException 如果等待过程中被中断
     */
    public ConfigUpdater(String hosts) throws IOException, InterruptedException {
        store = new ActiveKeyValueStore();
        store.connect(hosts);
    }

    /**
     * 运行更新循环
     * <p/>
     * 持续随机更新配置值,每次更新间隔1-10秒
     *
     * @throws InterruptedException 如果等待被中断
     * @throws KeeperException 如果ZooKeeper操作失败
     */
    public void run() throws InterruptedException, KeeperException {
        while (true) {
            // 生成0-99的随机字符串值
            String value = random.nextInt(100) + "";
            // 写入ZooKeeper
            store.write(PATH, value);
            System.out.printf("Set %s to %s\n", PATH, value);
            // 等待1-10秒后继续
            TimeUnit.SECONDS.sleep(random.nextInt(10));
        }
    }

    /**
     * 主程序入口
     *
     * @param args args[0]: ZooKeeper服务器地址
     */
    public static void main(String[] args) throws Exception {
        ConfigUpdater configUpdater = new ConfigUpdater(args[0]);
        configUpdater.run();
    }
}
// ^^ ConfigUpdater