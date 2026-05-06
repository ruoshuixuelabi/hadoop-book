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
 * 活跃键值存储类
 * <p>
 * 封装了对ZooKeeper节点的读写操作,提供简洁的API。
 * 使用ConnectionWatcher作为基类,继承ZooKeeper连接管理功能。
 * <p>
 * 主要功能:<br/>
 * - write(): 写入数据到指定路径(支持创建或更新)<br/>
 * - read(): 读取指定路径的数据(支持设置watcher)
 *
 * @see ConnectionWatcher
 * @see ResilientActiveKeyValueStore
 */
public class ActiveKeyValueStore extends ConnectionWatcher {

  /** UTF-8字符集编码 */
  private static final Charset CHARSET = Charset.forName("UTF-8");

//vv ActiveKeyValueStore-Write

  /**
   * 写入数据到指定路径
   * <p/>
   * 如果节点不存在则创建,存在则更新。-1表示使用任意版本号进行更新。
   *
   * @param path 节点路径
   * @param value 要写入的数据值
   * @throws InterruptedException 如果操作被中断
   * @throws KeeperException 如果ZooKeeper操作失败
   */
  public void write(String path, String value) throws InterruptedException,
      KeeperException {
    // 检查节点是否存在
    Stat stat = zk.exists(path, false);
    if (stat == null) {
      // 节点不存在,创建新节点
      zk.create(path, value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    } else {
      // 节点存在,-1表示不关心版本号,直接更新
      zk.setData(path, value.getBytes(CHARSET), -1);
    }
  }
//^^ ActiveKeyValueStore-Write
//^^ ActiveKeyValueStore

//vv ActiveKeyValueStore-Read

  /**
   * 读取指定路径的数据
   * <p/>
   * 设置watcher用于监视数据变化
   *
   * @param path 节点路径
   * @param watcher 用于监视数据变化的watcher,传入null表示不监视
   * @return 节点的数据内容
   * @throws InterruptedException 如果操作被中断
   * @throws KeeperException 如果ZooKeeper操作失败
   */
  public String read(String path, Watcher watcher) throws InterruptedException,
      KeeperException {
    // 读取数据并注册watcher用于监视变化
    byte[] data = zk.getData(path, watcher, null/*stat*/);
    return new String(data, CHARSET);
  }
//^^ ActiveKeyValueStore-Read
//vv ActiveKeyValueStore
}