//== ResilientConfigUpdater

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

/**
 * 可恢复的配置更新器
 *
 * 继承自ConfigUpdater,具有更强的容错能力。
 * 当ZooKeeper会话过期时,会自动创建新会话来恢复连接,
 * 而不会丢失正在运行的更新任务。
 *
 * 主要改进:
 * - 捕获会话过期异常并自动重连
 * - 持续运行直到遇到无法恢复的错误
 *
 * @see ConfigUpdater
 * @see ResilientActiveKeyValueStore
 */
public class ResilientConfigUpdater {

    /** 配置节点路径 */
    public static final String PATH = "/config";

    /** 可恢复的键值存储实例 */
    private ResilientActiveKeyValueStore store;

    /** 随机数生成器 */
    private Random random = new Random();

    /**
     * 构造函数
     *
     * @param hosts ZooKeeper服务器地址
     * @throws IOException 如果发生I/O错误
     * @throws InterruptedException 如果等待过程中被中断
     */
    public ResilientConfigUpdater(String hosts) throws IOException,
        InterruptedException {
        store = new ResilientActiveKeyValueStore();
        store.connect(hosts);
    }

    /**
     * 运行更新循环
     *
     * 持续随机更新配置值,每次更新间隔1-10秒
     *
     * @throws InterruptedException 如果等待被中断
     * @throws KeeperException 如果ZooKeeper操作失败
     */
    public void run() throws InterruptedException, KeeperException {
        while (true) {
            String value = random.nextInt(100) + "";
            store.write(PATH, value);
            System.out.printf("Set %s to %s\n", PATH, value);
            TimeUnit.SECONDS.sleep(random.nextInt(10));
        }
    }

//vv ResilientConfigUpdater

    /**
     * 主程序入口(带自动重连)
     *
     * 持续运行,当会话过期时自动重新连接。
     * 这使得程序可以在ZooKeeper服务器重启后自动恢复。
     *
     * @param args args[0]: ZooKeeper服务器地址
     */
    public static void main(String[] args) throws Exception {
        /*[*/while (true) {
            try {/*]*/
                ResilientConfigUpdater configUpdater =
                  new ResilientConfigUpdater(args[0]);
                configUpdater.run();
            /*[*/} catch (KeeperException.SessionExpiredException e) {
                // 会话过期,重新创建会话继续运行
                // start a new session
            } catch (KeeperException e) {
                // 其他错误(如连接失败),已重试过,退出循环
                // already retried, so exit
                e.printStackTrace();
                break;
            }
        }/*]*/
    }
//^^ ResilientConfigUpdater
}