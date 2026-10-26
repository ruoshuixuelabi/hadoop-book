//cc ConnectionWatcher A helper class that waits for the connection to ZooKeeper to be established
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

// vv ConnectionWatcher
/**
 * 等 ZooKeeper 连接建立好了再往下走的辅助类
 *
 * <p>{@link ZooKeeper} 的构造函数返回时，连接往往还没真正建好，
 * 所以这个类靠一个 {@link CountDownLatch} 把调用方卡住，
 * 等收到 {@code SyncConnected} 事件再放行
 * <p>本类自己实现了 {@link Watcher}，因为它得自己盯着连接状态
 */
public class ConnectionWatcher implements Watcher {
  
  // 会话超时时间，单位毫秒
  private static final int SESSION_TIMEOUT = 5000;

  // ZooKeeper 客户端句柄，子类直接用这个
  protected ZooKeeper zk;
  // 计数器：连接就绪时减到 0，等待的线程就被放行了
  private CountDownLatch connectedSignal = new CountDownLatch(1);

  /**
   * 连上 ZooKeeper 并等到连接就绪
   *
   * @param hosts 集群地址，例如 {@code localhost:2181}
   * @throws IOException 建立连接失败
   * @throws InterruptedException 等待被中断
   */
  public void connect(String hosts) throws IOException, InterruptedException {
    // 把自己作为监听器传进去，才能收到连接状态的变化
    zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
    connectedSignal.await();
  }
  
  /**
   * ZooKeeper 回调监听器的方法
   *
   * @param event 变化事件
   */
  @Override
  public void process(WatchedEvent event) {
    // 连接成功了才放行等待的线程
    if (event.getState() == KeeperState.SyncConnected) {
      connectedSignal.countDown();
    }
  }
  
  /**
   * 关闭连接，释放资源
   *
   * @throws InterruptedException 等待被中断
   */
  public void close() throws InterruptedException {
    // 关闭 ZooKeeper 客户端
    zk.close();
  }
}
// ^^ ConnectionWatcher