//cc ConfigWatcher 一个监视ZooKeeper中配置属性更新并打印到控制台的应用程序

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

// vv ConfigWatcher

/**
 * 配置监视器程序
 * <p>
 * 该程序实现Watcher接口,用于监视ZooKeeper中指定节点的数据变化。
 * 当节点数据发生更新时,自动读取新数据并打印到控制台。
 * <p>
 * 使用说明:<br/>
 * java ConfigWatcher <zookeeper地址>
 * <p>
 * 工作流程:<br/>
 * 1. 连接到ZooKeeper服务器<br/>
 * 2. 读取/config节点的数据并显示<br/>
 * 3. 设置watch监视数据变化<br/>
 * 4. 当数据变化时,自动重新读取并显示
 *
 * @example
 * java ConfigWatcher localhost:2181
 * // 输出: Read /config as 42
 * // 等待更新...
 *
 * @see ConfigUpdater
 */
public class ConfigWatcher implements Watcher {

    /** 活跃的键值存储实例 */
    private ActiveKeyValueStore store;

    /**
     * 构造函数
     * <p/>
     * 创建ActiveKeyValueStore实例并连接到ZooKeeper服务器
     *
     * @param hosts ZooKeeper服务器地址
     * @throws IOException 如果发生I/O错误
     * @throws InterruptedException 如果等待过程中被中断
     */
    public ConfigWatcher(String hosts) throws IOException, InterruptedException {
        store = new ActiveKeyValueStore();
        store.connect(hosts);
    }

    /**
     * 显示当前配置值
     * <p/>
     * 读取/config节点的数据并打印到控制台
     *
     * @throws InterruptedException 如果读取被中断
     * @throws KeeperException 如果ZooKeeper操作失败
     */
    public void displayConfig() throws InterruptedException, KeeperException {
        String value = store.read(ConfigUpdater.PATH, this);
        System.out.printf("Read %s as %s\n", ConfigUpdater.PATH, value);
    }

    /**
     * 处理ZooKeeper事件回调
     * <p/>
     * 当监视的节点数据发生变化时,自动重新读取并显示新值
     *
     * @param event ZooKeeper事件
     */
    @Override
    public void process(WatchedEvent event) {
        // 只处理节点数据变化事件
        if (event.getType() == EventType.NodeDataChanged) {
            try {
                // 重新读取配置并显示
                displayConfig();
            }
            catch (InterruptedException e) {
                // 被中断,退出
                System.err.println("Interrupted. Exiting.");
                Thread.currentThread().interrupt();
            }
            catch (KeeperException e) {
                // ZooKeeper错误,输出错误信息并退出
                System.err.printf("KeeperException: %s. Exiting.\n", e);
            }
        }
    }

    /**
     * 主程序入口
     *
     * @param args args[0]: ZooKeeper服务器地址
     */
    public static void main(String[] args) throws Exception {
        ConfigWatcher configWatcher = new ConfigWatcher(args[0]);
        configWatcher.displayConfig();
        // 保持运行直到进程被杀死或线程被中断
        Thread.sleep(Long.MAX_VALUE);
    }
}
//^^ ConfigWatcher