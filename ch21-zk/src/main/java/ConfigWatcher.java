//cc ConfigWatcher An application that watches for updates of a property in ZooKeeper and prints them to the console
import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

// vv ConfigWatcher
/**
 * 盯着 ZooKeeper 里的配置节点，一有变化就把新值打印出来
 *
 * <p>用 {@link Watcher} 接口实现监听：
 * 每次读的时候顺手把自己挂上去，节点一改，ZooKeeper 就会回调 {@link #process}
 * <p>配合 {@link ConfigUpdater} 使用效果最好
 */
public class ConfigWatcher implements Watcher {
  
  // 用来读配置的键值存储
  private ActiveKeyValueStore store;
  
  /**
   * 建好连接，准备开始监听
   *
   * @param hosts ZooKeeper 集群地址，例如 {@code localhost:2181}
   * @throws IOException 建立连接失败
   * @throws InterruptedException 等待被中断
   */
  public ConfigWatcher(String hosts) throws IOException, InterruptedException {
    store = new ActiveKeyValueStore();
    // 这一句会一直等到连接就绪才返回
    store.connect(hosts);
  }
  
  /**
   * 读一次配置并打印出来
   *
   * <p>读的时候把 {@code this} 这个监听器传进去，
   * 这样节点值一改，ZooKeeper 就会再回调一次
   */
  public void displayConfig() throws InterruptedException, KeeperException {
    // 带上 this，节点变了才收得到通知
    String value = store.read(ConfigUpdater.PATH, this);
    System.out.printf("Read %s as %s\n", ConfigUpdater.PATH, value);
  }

  /**
   * ZooKeeper 回调监听器的方法
   *
   * <p>只有节点的数据发生变化（{@code NodeDataChanged}）时才重新读一次；
   * 子节点增减之类的变化这里不关心
   *
   * @param event 变化事件
   */
  @Override
  public void process(WatchedEvent event) {
    // 只关心数据变了，别的变化不管
    if (event.getType() == EventType.NodeDataChanged) {
      try {
        displayConfig();
      } catch (InterruptedException e) {
        System.err.println("Interrupted. Exiting.");        
        // 恢复中断标志，别把中断信号吞掉
        Thread.currentThread().interrupt();
      } catch (KeeperException e) {
        System.err.printf("KeeperException: %s. Exiting.\n", e);        
      }
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
    ConfigWatcher configWatcher = new ConfigWatcher(args[0]);
    configWatcher.displayConfig();
    
    // stay alive until process is killed or thread is interrupted
    // 补充：主线程在这里一直挂着，配置变化靠 ZooKeeper 的回调线程来响应
    Thread.sleep(Long.MAX_VALUE);
  }
}
//^^ ConfigWatcher
