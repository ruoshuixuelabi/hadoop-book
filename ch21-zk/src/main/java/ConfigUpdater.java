/**
 * 文件：ConfigUpdater.java
 * 路径：src/main/java/ConfigUpdater.java
 * 描述：在随机时间更新 ZooKeeper 中配置属性的应用程序
 * <p>
 * 功能说明：
 * 该程序定期随机更新 ZooKeeper 中 /config 节点的数据。
 * 用于演示 ZooKeeper 的配置管理功能和 watch 机制。
 * <p>
 * 使用说明：
 * java ConfigUpdater <zookeeper地址>
 * <p>
 * 工作流程：
 * 1. 连接到 ZooKeeper 服务器
 * 2. 每隔 1-10 秒随机生成一个 0-99 的数值
 * 3. 将新值写入 /config 节点
 * 4. 重复执行
 * <p>
 * @example java ConfigUpdater localhost:2181
 *          // 输出: Set /config to 42
 *          //       Set /config to 17
 *          //       ...
 */
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

/**
 * 配置更新器程序
 * <p>
 * 该程序定期随机更新 ZooKeeper 中 /config 节点的数据。
 * 用于演示 ZooKeeper 的配置管理功能和 watch 机制。
 * <p>
 * 使用说明：
 * java ConfigUpdater <zookeeper地址>
 * <p>
 * 工作流程：
 * 1. 连接到 ZooKeeper 服务器
 * 2. 每隔 1-10 秒随机生成一个 0-99 的数值
 * 3. 将新值写入 /config 节点
 * 4. 重复执行
 */
public class ConfigUpdater {

    /**
     * 配置节点路径<br>
     * 在 ZooKeeper 中存储配置数据的节点路径
     */
    public static final String PATH = "/config";

    /**
     * 活跃的键值存储实例<br>
     * 用于与 ZooKeeper 服务器通信
     */
    private ActiveKeyValueStore store;

    /**
     * 随机数生成器<br>
     * 用于生成随机的配置值
     */
    private Random random = new Random();

    /**
     * 构造函数<br>
     * <p>
     * 创建 ActiveKeyValueStore 实例并连接到 ZooKeeper 服务器。
     * <p>
     * @param hosts ZooKeeper 服务器地址
     * @throws IOException 如果发生 I/O 错误
     * @throws InterruptedException 如果等待过程中被中断
     */
    public ConfigUpdater(String hosts) throws IOException, InterruptedException {
        // 创建 ActiveKeyValueStore 实例
        store = new ActiveKeyValueStore();
        // 连接到 ZooKeeper 服务器
        store.connect(hosts);
    }

    /**
     * 运行更新循环<br>
     * <p>
     * 持续随机更新配置值，每次更新间隔 1-10 秒。
     * <p>
     * @throws InterruptedException 如果等待被中断
     * @throws KeeperException 如果 ZooKeeper 操作失败
     */
    public void run() throws InterruptedException, KeeperException {
        // 持续运行更新循环
        while (true) {
            // 生成 0-99 的随机字符串值
            String value = random.nextInt(100) + "";
            // 写入 ZooKeeper
            store.write(PATH, value);
            // 打印更新后的值
            System.out.printf("Set %s to %s\n", PATH, value);
            // 等待 1-10 秒后继续
            TimeUnit.SECONDS.sleep(random.nextInt(10));
        }
    }

    /**
     * 主程序入口<br>
     * <p>
     * @param args args[0]: ZooKeeper 服务器地址
     */
    public static void main(String[] args) throws Exception {
        // 创建 ConfigUpdater 实例
        ConfigUpdater configUpdater = new ConfigUpdater(args[0]);
        // 运行更新循环
        configUpdater.run();
    }
}