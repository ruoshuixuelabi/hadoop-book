//cc JoinGroup A program that joins a group

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

// vv JoinGroup
/**
 * 加入一个群组：在群组节点下面建一个以自己命名的临时节点
 *
 * <p>节点是临时的（{@link CreateMode#EPHEMERAL}），
 * 进程一退出节点自动消失，也就等于自动退出了群组，
 * 不用另外写「退群」的代码
 */
public class JoinGroup extends ConnectionWatcher {
  
  /**
   * 加入群组
   *
   * @param groupName 群组名
   * @param memberName 成员名，会被拼成 {@code /群组名/成员名}
   * @throws KeeperException 与 ZooKeeper 通信失败
   * @throws InterruptedException 等待被中断
   */
  public void join(String groupName, String memberName) throws KeeperException,
      InterruptedException {
    // 成员节点挂在群组节点下面
    String path = "/" + groupName + "/" + memberName;
    // 成员节点本身也不存数据
    String createdPath = zk.create(path, null/*data*/, Ids.OPEN_ACL_UNSAFE,
      CreateMode.EPHEMERAL);
    // 打印真正建出来的路径
    System.out.println("Created " + createdPath);
  }
  
  /**
   * 程序入口
   *
   * @param args 第一个参数是集群地址，第二个参数是群组名，第三个参数是成员名
   * @throws Exception 运行失败
   */
  public static void main(String[] args) throws Exception {
    JoinGroup joinGroup = new JoinGroup();
    // 连上 ZooKeeper 并等连接就绪
    joinGroup.connect(args[0]);
    // 入群：群组名和成员名分别来自第 1、2 个参数
    joinGroup.join(args[1], args[2]);
    
    // stay alive until process is killed or thread is interrupted
    // 补充：主线程一直挂着，进程才不会被立刻结束掉
    Thread.sleep(Long.MAX_VALUE);
  }
}
// ^^ JoinGroup
