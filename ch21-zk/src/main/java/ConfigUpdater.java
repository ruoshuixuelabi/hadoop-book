//cc ConfigUpdater An application that updates a property in ZooKeeper at random times
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

// vv ConfigUpdater
/**
 * 定期往 ZooKeeper 里写一个新值，模拟「配置在不断变化」
 *
 * <p>配套的读者是 {@link ConfigWatcher}：
 * 这个程序负责改，{@code ConfigWatcher} 负责盯
 * <p>值写在哪里由 {@link #PATH} 决定，
 * 写多频繁则由随机数决定，这样演示效果更像真实场景
 */
public class ConfigUpdater {
  
  // 存放配置的节点路径，读写双方约定好这一个
  public static final String PATH = "/config";
  
  // 真正干活的键值存储
  private ActiveKeyValueStore store;
  // 随机数发生器，决定每次写什么值、隔多久写一次
  private Random random = new Random();
  
  /**
   * 建好连接，准备开始写配置
   *
   * @param hosts ZooKeeper 集群地址，例如 {@code localhost:2181}
   * @throws IOException 建立连接失败
   * @throws InterruptedException 等待被中断
   */
  public ConfigUpdater(String hosts) throws IOException, InterruptedException {
    store = new ActiveKeyValueStore();
    // 这一句会一直等到连接就绪才返回
    store.connect(hosts);
  }
  
  /**
   * 一直写下去，每隔几秒写一个新的随机值
   *
   * @throws InterruptedException 等待被中断
   * @throws KeeperException 与 ZooKeeper 通信失败
   */
  public void run() throws InterruptedException, KeeperException {
    // 无限期循环，除非进程被杀掉
    while (true) {
      // 0~99 之间的随机数
      String value = random.nextInt(100) + "";
      // 写进 ZooKeeper
      store.write(PATH, value);
      System.out.printf("Set %s to %s\n", PATH, value);
      // 随机等 0~9 秒再写下一次
      TimeUnit.SECONDS.sleep(random.nextInt(10));
    }
  }
  
  /**
   * 程序入口
   *
   * @param args 第一个参数是 ZooKeeper 集群地址
   * @throws Exception 运行失败
   */
  public static void main(String[] args) throws Exception {
    // 用第 0 个参数当集群地址
    ConfigUpdater configUpdater = new ConfigUpdater(args[0]);
    configUpdater.run();
  }
}
// ^^ ConfigUpdater
