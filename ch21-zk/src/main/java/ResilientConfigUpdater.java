//== ResilientConfigUpdater
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

/**
 * 会在会话过期后自动重连的配置更新程序
 *
 * <p>和 {@link ConfigUpdater} 的区别有两处：
 * 底层用的是 {@link ResilientActiveKeyValueStore}，写入失败会自己重试；
 * 外层还套了一个死循环，会话过期就换个新会话从头再来
 */
public class ResilientConfigUpdater {
  
  // 存放配置的节点路径，读写双方约定好这一个
  public static final String PATH = "/config";
  
  // 带重试的键值存储
  private ResilientActiveKeyValueStore store;
  // 随机数发生器，决定每次写什么值、隔多久写一次
  private Random random = new Random();
  
  /**
   * 建好连接，准备开始写配置
   *
   * @param hosts ZooKeeper 集群地址，例如 {@code localhost:2181}
   * @throws IOException 建立连接失败
   * @throws InterruptedException 等待被中断
   */
  public ResilientConfigUpdater(String hosts) throws IOException,
      InterruptedException {
    store = new ResilientActiveKeyValueStore();
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
      // 写进 ZooKeeper，失败的话由存储层自己重试
      store.write(PATH, value);
      System.out.printf("Set %s to %s\n", PATH, value);
      // 随机等 0~9 秒再写下一次
      TimeUnit.SECONDS.sleep(random.nextInt(10));
    }
  }
  
//vv ResilientConfigUpdater
  /**
   * 程序入口：会话过期了就重来一轮
   *
   * @param args 第一个参数是 ZooKeeper 集群地址
   * @throws Exception 运行失败
   */
  public static void main(String[] args) throws Exception {
    // 外层死循环：会话过期也不退出，换个新会话再来
    /*[*/while (true) {
      try {/*]*/
        ResilientConfigUpdater configUpdater =
          new ResilientConfigUpdater(args[0]);
        configUpdater.run();
      // 会话过期：连接本身已经废了，得开一个新会话重连
      /*[*/} catch (KeeperException.SessionExpiredException e) {
        // start a new session
      // 别的异常：存储层已经重试过了，这里只能放弃
      } catch (KeeperException e) {
        // already retried, so exit
        // 打印异常堆栈，方便排查
        e.printStackTrace();
        // 跳出重试循环
        break;
      }
    }/*]*/
  }
//^^ ResilientConfigUpdater
}
