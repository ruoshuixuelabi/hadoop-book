//cc JoinGroup 一个加入组的程序

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

// vv JoinGroup

/**
 * 加入ZooKeeper组的程序
 * <p>
 * 该程序继承自ConnectionWatcher,用于将当前成员加入到指定的组中。
 * 成员节点采用临时(Ephemeral)模式创建,当客户端断开连接时自动删除。
 * 这使得ZooKeeper可以自动检测成员是否存活。
 * <p>
 * 使用说明:<br/>
 * java JoinGroup <zookeeper地址> <组名> <成员名>
 *
 * @example
 * java JoinGroup localhost:2181 mygroup member1
 * // 将在ZooKeeper中创建临时节点: /mygroup/member1
 * <p>
 * 注意: 程序会阻塞等待,直到进程被杀死或线程被中断
 *
 * @see ConnectionWatcher
 */
public class JoinGroup extends ConnectionWatcher {

    /**
     * 加入指定组
     * <p/>
     * 创建一个临时节点来表示组成员。临时节点会在客户端断开连接时自动删除,<br/>
     * 这使得ZooKeeper能够自动检测成员是否存活。
     *
     * @param groupName 组名称(不含前导斜杠)
     * @param memberName 成员名称
     * @throws KeeperException 如果ZooKeeper操作失败
     * @throws InterruptedException 如果操作被中断
     */
    public void join(String groupName, String memberName) throws KeeperException,
            InterruptedException {
        String path = "/" + groupName + "/" + memberName;
        // 创建临时节点,用于组成员心跳检测
        String createdPath = zk.create(path, null/*data*/, Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        System.out.println("Created " + createdPath);
    }

    /**
     * 主程序入口
     *
     * @param args args[0]: ZooKeeper服务器地址
     *             args[1]: 组名称
     *             args[2]: 成员名称
     */
    public static void main(String[] args) throws Exception {
        JoinGroup joinGroup = new JoinGroup();
        joinGroup.connect(args[0]);
        joinGroup.join(args[1], args[2]);

        // 保持运行直到进程被杀死或线程被中断
        // 这样可以维持ZooKeeper连接,确保临时节点不被删除
        Thread.sleep(Long.MAX_VALUE);
    }
}
// ^^ JoinGroup