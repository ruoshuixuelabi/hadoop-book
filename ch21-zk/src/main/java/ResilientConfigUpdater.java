/**
 * 文件：ResilientConfigUpdater.java
 * 路径：src/main/java/ResilientConfigUpdater.java
 * 描述：可恢复的配置更新器
 * <p>
 * 功能说明：
 * 继承自 ConfigUpdater，具有更强的容错能力。
 * 当 ZooKeeper 会话过期时，会自动创建新会话来恢复连接，
 * 而不会丢失正在运行的更新任务。
 * <p>
 * 主要改进：
 * - 捕获会话过期异常并自动重连
 * - 持续运行直到遇到无法恢复的错误
 * <p>
 * @see ConfigUpdater
 * @see ResilientActiveKeyValueStore
 */
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

/**
 * 可恢复的配置更新器
 * <p>
 * 继承自 ConfigUpdater，具有更强的容错能力。
 * 当 ZooKeeper 会话过期时，会自动创建新会话来恢复连接，
 * 而不会丢失正在运行的更新任务。
 * <p>
 * 主要改进：
 * - 捕获会话过期异常并自动重连
 * - 持续运行直到遇到无法恢复的错误
 */
public class ResilientConfigUpdater {

    /**
     * 配置节点路径<br>
     * 在 ZooKeeper 中存储配置数据的节点路径
     */
    public static final String PATH = "/config";

    /**
     * 可恢复的键值存储实例<br>
     * 用于与 ZooKeeper 服务器通信，具有重试机制
     */
    private ResilientActiveKeyValueStore store;

    /**
     * 随机数生成器<br>
     * 用于生成随机的配置值
     */
    private Random random = new Random();

    /**
     * 构造函数<br>
     * <p>
     * 创建 ResilientActiveKeyValueStore 实例并连接到 ZooKeeper 服务器。
     * <p>
     * @param hosts ZooKeeper 服务器地址
     * @throws IOException 如果发生 I/O 错误
     * @throws InterruptedException 如果等待过程中被中断
     */
    public ResilientConfigUpdater(String hosts) throws IOException,
            InterruptedException {
        // 创建 ResilientActiveKeyValueStore 实例
        store = new ResilientActiveKeyValueStore();
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
            // 写入 ZooKeeper（带重试机制）
            store.write(PATH, value);
            // 打印更新后的值
            System.out.printf("Set %s to %s\n", PATH, value);
            // 等待 1-10 秒后继续
            TimeUnit.SECONDS.sleep(random.nextInt(10));
        }
    }

    /**
     * 主程序入口（带自动重连）<br>
     * <p>
     * 持续运行，当会话过期时自动重新连接。
     * 这使得程序可以在 ZooKeeper 服务器重启后自动恢复。
     * <p>
     * @param args args[0]: ZooKeeper 服务器地址
     */
    public static void main(String[] args) throws Exception {
        // 外层循环用于处理会话过期后的重连
        while (true) {
            try {
                // 创建 ResilientConfigUpdater 实例
                ResilientConfigUpdater configUpdater =
                        new ResilientConfigUpdater(args[0]);
                // 运行更新循环
                configUpdater.run();
            } catch (KeeperException.SessionExpiredException e) {
                // 会话过期，重新创建会话继续运行
                // ZooKeeper 会话过期后需要重新创建连接
                System.out.println("Session expired, reconnecting...");
            } catch (KeeperException e) {
                // 其他错误（如连接失败），打印错误信息并退出
                // 这些错误通常不是暂时性的
                e.printStackTrace();
                break;
            }
        }
    }
}