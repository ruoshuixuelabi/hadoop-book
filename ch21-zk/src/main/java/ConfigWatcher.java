/**
 * 文件：ConfigWatcher.java
 * 路径：src/main/java/ConfigWatcher.java
 * 描述：监视 ZooKeeper 中配置属性更新并打印到控制台的应用程序
 * <p>
 * 功能说明：
 * 该程序实现 Watcher 接口，用于监视 ZooKeeper 中指定节点的数据变化。
 * 当节点数据发生更新时，自动读取新数据并打印到控制台。
 * <p>
 * 使用说明：
 * java ConfigWatcher <zookeeper地址>
 * <p>
 * 工作流程：
 * 1. 连接到 ZooKeeper 服务器
 * 2. 读取 /config 节点的数据并显示
 * 3. 设置 watch 监视数据变化
 * 4. 当数据变化时，自动重新读取并显示
 * <p>
 * @example java ConfigWatcher localhost:2181
 *          // 输出: Read /config as 42
 *          // 等待更新...
 */
import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * 配置监视器程序
 * <p>
 * 该程序实现 Watcher 接口，用于监视 ZooKeeper 中指定节点的数据变化。
 * 当节点数据发生更新时，自动读取新数据并打印到控制台。
 * <p>
 * 使用说明：
 * java ConfigWatcher <zookeeper地址>
 * <p>
 * 工作流程：
 * 1. 连接到 ZooKeeper 服务器
 * 2. 读取 /config 节点的数据并显示
 * 3. 设置 watch 监视数据变化
 * 4. 当数据变化时，自动重新读取并显示
 */
public class ConfigWatcher implements Watcher {

    /**
     * 活跃的键值存储实例<br>
     * 用于与 ZooKeeper 服务器通信
     */
    private ActiveKeyValueStore store;

    /**
     * 构造函数<br>
     * <p>
     * 创建 ActiveKeyValueStore 实例并连接到 ZooKeeper 服务器。
     * <p>
     * @param hosts ZooKeeper 服务器地址
     * @throws IOException 如果发生 I/O 错误
     * @throws InterruptedException 如果等待过程中被中断
     */
    public ConfigWatcher(String hosts) throws IOException, InterruptedException {
        // 创建 ActiveKeyValueStore 实例
        store = new ActiveKeyValueStore();
        // 连接到 ZooKeeper 服务器
        store.connect(hosts);
    }

    /**
     * 显示当前配置值<br>
     * <p>
     * 读取 /config 节点的数据并打印到控制台。
     * <p>
     * @throws InterruptedException 如果读取被中断
     * @throws KeeperException 如果 ZooKeeper 操作失败
     */
    public void displayConfig() throws InterruptedException, KeeperException {
        // 读取配置数据，并设置当前 watcher 监视变化
        String value = store.read(ConfigUpdater.PATH, this);
        // 打印读取的配置值
        System.out.printf("Read %s as %s\n", ConfigUpdater.PATH, value);
    }

    /**
     * 处理 ZooKeeper 事件回调<br>
     * <p>
     * 当监视的节点数据发生变化时，自动重新读取并显示新值。
     * <p>
     * @param event ZooKeeper 事件对象
     */
    @Override
    public void process(WatchedEvent event) {
        // 只处理节点数据变化事件
        // EventType.NodeDataChanged 表示节点数据发生变化
        if (event.getType() == EventType.NodeDataChanged) {
            try {
                // 重新读取配置并显示
                displayConfig();
            }
            catch (InterruptedException e) {
                // 被中断，退出
                System.err.println("Interrupted. Exiting.");
                Thread.currentThread().interrupt();
            }
            catch (KeeperException e) {
                // ZooKeeper 错误，输出错误信息并退出
                System.err.printf("KeeperException: %s. Exiting.\n", e);
            }
        }
    }

    /**
     * 主程序入口<br>
     * <p>
     * @param args args[0]: ZooKeeper 服务器地址
     */
    public static void main(String[] args) throws Exception {
        // 创建 ConfigWatcher 实例
        ConfigWatcher configWatcher = new ConfigWatcher(args[0]);
        // 显示当前配置
        configWatcher.displayConfig();
        // 保持运行直到进程被杀死或线程被中断
        // 这样可以持续监视配置变化
        Thread.sleep(Long.MAX_VALUE);
    }
}