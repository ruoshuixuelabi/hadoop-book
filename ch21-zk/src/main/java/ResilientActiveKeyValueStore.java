//== ResilientActiveKeyValueStore-Write

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * 会重试的键值存储：写入失败时自动重试
 *
 * <p>{@link ActiveKeyValueStore} 遇到异常就直接往上抛，
 * 这个版本在网络抖动时会自己重试若干次
 * <p>但会话过期（{@code SessionExpiredException}）不重试，
 * 因为那说明连接本身已经废了，再重试也没用
 */
public class ResilientActiveKeyValueStore extends ConnectionWatcher {

  // ZooKeeper 存的是字节数组，这里统一用 UTF-8
  private static final Charset CHARSET = Charset.forName("UTF-8");
  // 最多重试几次
  private static final int MAX_RETRIES = 5;
  // 每次重试前等多久，单位秒
  private static final int RETRY_PERIOD_SECONDS = 10;

//vv ResilientActiveKeyValueStore-Write
  /**
   * 把一个键值对写进 ZooKeeper，失败会自动重试
   *
   * @param path 节点路径
   * @param value 要存的值
   * @throws InterruptedException 等待被中断
   * @throws KeeperException 重试若干次之后仍然失败
   */
  public void write(String path, String value) throws InterruptedException,
      KeeperException {
    // 已经重试了几次
    int retries = 0;
    // 一直试到成功，或者重试次数用光
    while (true) {
      try {
        // 节点在不在，决定这次是新建还是改写
        Stat stat = zk.exists(path, false);
        // 不在就新建
        if (stat == null) {
          zk.create(path, value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
        // 在就覆盖旧值
        } else {
          // 用刚才查到的版本号，避免把别人的修改覆盖掉
          zk.setData(path, value.getBytes(CHARSET), stat.getVersion());
        }
        return;
      // 会话过期了，重试也没用，直接往上抛
      } catch (KeeperException.SessionExpiredException e) {
        throw e;
      // 别的异常还有救，试试重试
      } catch (KeeperException e) {
        // 重试次数用完了，只能放弃
        if (retries++ == MAX_RETRIES) {
          throw e;
        }
        // sleep then retry
        // 补充：等一会儿再重试，给集群一点恢复的时间
        TimeUnit.SECONDS.sleep(RETRY_PERIOD_SECONDS);
      }
    }
  }
//^^ ResilientActiveKeyValueStore-Write
  /**
   * 读取一个节点的值
   *
   * <p>和 {@link ActiveKeyValueStore#read} 一样，这里不做重试：
   * 读失败通常也是会话出了问题，重试意义不大
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
}
