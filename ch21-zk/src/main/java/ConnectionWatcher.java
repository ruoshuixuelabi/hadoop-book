//cc ConnectionWatcher 一个帮助类,用于等待与ZooKeeper的连接建立

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

// vv ConnectionWatcher

/**
 * ZooKeeper连接监视器类
 *
 * 该类实现了Watcher接口,用于处理ZooKeeper连接事件。
 * 主要功能是等待ZooKeeper客户端与服务器之间的连接成功建立。
 *
 * 使用说明:
 * 1. 创建ConnectionWatcher实例
 * 2. 调用connect(hosts)方法连接到ZooKeeper服务器
 * 3. 连接建立后,connectedSignal倒计数器会归零,表示连接就绪
 * 4. 操作完成后调用close()方法关闭连接
 *
 * @example
 * ConnectionWatcher watcher = new ConnectionWatcher();
 * watcher.connect("localhost:2181");
 * // 执行ZooKeeper操作...
 * watcher.close();
 */
public class ConnectionWatcher implements Watcher {

    /** 会话超时时间,单位毫秒 */
    private static final int SESSION_TIMEOUT = 5000;

    /** ZooKeeper客户端实例 */
    protected ZooKeeper zk;

    /** 连接信号量,用于阻塞等待连接建立 */
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    /**
     * 连接到ZooKeeper服务器
     *
     * @param hosts ZooKeeper服务器地址列表,格式如"host1:port1,host2:port2"
     * @throws IOException 如果发生I/O错误
     * @throws InterruptedException 如果等待过程中被中断
     */
    public void connect(String hosts) throws IOException, InterruptedException {
        // 创建ZooKeeper客户端,注册当前watcher
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        // 阻塞等待连接建立完成
        connectedSignal.await();
    }

    /**
     * 处理ZooKeeper事件回调
     *
     * 当收到SyncConnected状态事件时,表示连接已建立,释放信号量
     *
     * @param event ZooKeeper事件
     */
    @Override
    public void process(WatchedEvent event) {
        // 检查是否为连接建立事件
        if (event.getState() == KeeperState.SyncConnected) {
            // 连接已建立,倒计数归零,解除阻塞
            connectedSignal.countDown();
        }
    }

    /**
     * 关闭ZooKeeper连接
     *
     * @throws InterruptedException 如果关闭过程中被中断
     */
    public void close() throws InterruptedException {
        zk.close();
    }
}
// ^^ ConnectionWatcher