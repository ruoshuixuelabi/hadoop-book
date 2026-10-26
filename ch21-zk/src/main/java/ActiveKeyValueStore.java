//== ActiveKeyValueStore
//== ActiveKeyValueStore-Read
//== ActiveKeyValueStore-Write

import java.nio.charset.Charset;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

// vv ActiveKeyValueStore
/**
 * 键值存储：把数据写进 ZooKeeper 的 znode
 *
 * <p>所谓「活的」（active）存储，是指数据真的落在 ZooKeeper 集群里，
 * 程序退出之后数据还在，而不是存在本地文件里
 * <p>建连接、等连接就绪这些事交给父类 {@link ConnectionWatcher} 去做，
 * 这里只提供读、写两个动作
 */
public class ActiveKeyValueStore extends ConnectionWatcher {

  // ZooKeeper 存的是字节数组，这里约定统一用 UTF-8 编码
  private static final Charset CHARSET = Charset.forName("UTF-8");

//vv ActiveKeyValueStore-Write
  /**
   * 把一个键值对写进 ZooKeeper
   *
   * <p>节点不存在就创建，存在就覆盖原来的值；
   * 这样调用方不用关心这个键是第一次写还是第几次写
   *
   * @param path 节点路径，例如 {@code /config}
   * @param value 要存的值，内部会转成 UTF-8 字节
   * @throws InterruptedException 等待被中断
   * @throws KeeperException 与 ZooKeeper 通信失败
   */
  public void write(String path, String value) throws InterruptedException,
      KeeperException {
    // 先看看这个节点在不在
    Stat stat = zk.exists(path, false);
    // 不在，说明要新建
    if (stat == null) {
      // 用 PERSISTENT 模式，节点建好之后就一直存在
      zk.create(path, value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    // 已经存在，覆盖旧的值
    } else {
      // 版本号传 -1，表示不管有没有别人改过
      zk.setData(path, value.getBytes(CHARSET), -1);
    }
  }
//^^ ActiveKeyValueStore-Write
//^^ ActiveKeyValueStore
//vv ActiveKeyValueStore-Read
  /**
   * 读取一个节点的值
   *
   * <p>注意这里的 watcher 参数：传监听器就能在节点值变化时收到通知，
   * 传 {@code null} 就是单纯读一次
   *
   * @param path 节点路径
   * @param watcher 监听器；传 {@code null} 表示不监听
   * @return 节点里存的字符串
   * @throws InterruptedException 等待被中断
   * @throws KeeperException 与 ZooKeeper 通信失败
   */
  public String read(String path, Watcher watcher) throws InterruptedException,
      KeeperException {
    // 最后一个参数是「要不要顺便回传节点状态」，这里不需要
    byte[] data = zk.getData(path, watcher, null/*stat*/);
    return new String(data, CHARSET);
  }
//^^ ActiveKeyValueStore-Read
//vv ActiveKeyValueStore
}
//^^ ActiveKeyValueStore
