//cc DeleteGroup A program to delete a group and its members
import java.util.List;

import org.apache.zookeeper.KeeperException;

// vv DeleteGroup
/**
 * 删掉一个群组，以及群组里的所有成员
 *
 * <p>ZooKeeper 不允许直接删一个还带着子节点的节点，
 * 所以得先把成员一个个删掉，再删群组本身
 */
public class DeleteGroup extends ConnectionWatcher {
    
  /**
   * 删掉群组和它的所有成员
   *
   * @param groupName 群组名
   * @throws KeeperException 与 ZooKeeper 通信失败
   * @throws InterruptedException 等待被中断
   */
  public void delete(String groupName) throws KeeperException,
      InterruptedException {
    // 群组节点的完整路径
    String path = "/" + groupName;
    
    try {
      // 先列出群组里的所有成员
      List<String> children = zk.getChildren(path, false);
      // 逐个删掉成员
      for (String child : children) {
        // 版本号传 -1，表示不管版本
        zk.delete(path + "/" + child, -1);
      }
      // 成员都删完了，再删群组本身
      zk.delete(path, -1);
    // 群组本来就不存在
    } catch (KeeperException.NoNodeException e) {
      System.out.printf("Group %s does not exist\n", groupName);
      // 用非零退出码表示出错
      System.exit(1);
    }
  }
  
  /**
   * 程序入口
   *
   * @param args 第一个参数是集群地址，第二个参数是群组名
   * @throws Exception 运行失败
   */
  public static void main(String[] args) throws Exception {
    DeleteGroup deleteGroup = new DeleteGroup();
    // 连上 ZooKeeper 并等连接就绪
    deleteGroup.connect(args[0]);
    // 删群组
    deleteGroup.delete(args[1]);
    deleteGroup.close();
  }
}
// ^^ DeleteGroup
