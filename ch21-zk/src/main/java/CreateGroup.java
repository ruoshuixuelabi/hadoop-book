//cc CreateGroup A program to create a znode representing a group in ZooKeeper

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

// vv CreateGroup
/**
 * 在 ZooKeeper 里创建一个代表「群组」的节点
 *
 * <p>群组的成员就是群组节点下面的子节点，
 * 成员怎么加进来见 {@link JoinGroup}
 * <p>这个类没有继承 {@link ConnectionWatcher}，
 * 因为它自己实现了 {@link Watcher} 来等连接就绪
 */
public class CreateGroup implements Watcher {
  
  // 会话超时时间，单位毫秒
  private static final int SESSION_TIMEOUT = 5000;
  
  // ZooKeeper 客户端句柄
  private ZooKeeper zk;
  // 计数器：连接就绪时减到 0
  private CountDownLatch connectedSignal = new CountDownLatch(1);
  
  /**
   * 连上 ZooKeeper 并等到连接就绪
   *
   * @param hosts 集群地址，例如 {@code localhost:2181}
   * @throws IOException 建立连接失败
   * @throws InterruptedException 等待被中断
   */
  public void connect(String hosts) throws IOException, InterruptedException {
    // 把自己作为监听器传进去
    zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
    connectedSignal.await();
  }
  
  /**
   * ZooKeeper 回调监听器的方法
   *
   * @param event 变化事件
   */
  @Override
  public void process(WatchedEvent event) { // Watcher interface
    // 连接成功了才放行等待的线程
    if (event.getState() == KeeperState.SyncConnected) {
      connectedSignal.countDown();
    }
  }
  
  /**
   * 创建一个群组节点
   *
   * <p>群组节点建好之后是空的，成员要另外加进来
   *
   * @param groupName 群组名，会被拼成 {@code /群组名}
   * @throws KeeperException 与 ZooKeeper 通信失败
   * @throws InterruptedException 等待被中断
   */
  public void create(String groupName) throws KeeperException,
      InterruptedException {
    // 群组节点就是根节点下的一个子节点
    String path = "/" + groupName;
    // 群组节点本身不存数据
    String createdPath = zk.create(path, null/*data*/, Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    // 打印真正建出来的路径
    System.out.println("Created " + createdPath);
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

  /**
   * 程序入口
   *
   * @param args 第一个参数是集群地址，第二个参数是群组名
   * @throws Exception 运行失败
   */
  public static void main(String[] args) throws Exception {
    CreateGroup createGroup = new CreateGroup();
    // 连上 ZooKeeper 并等连接就绪
    createGroup.connect(args[0]);
    // 建群组
    createGroup.create(args[1]);
    createGroup.close();
  }
}
// ^^ CreateGroup
