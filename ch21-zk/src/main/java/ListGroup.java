//cc ListGroup A program to list the members in a group
import java.util.List;

import org.apache.zookeeper.KeeperException;

// vv ListGroup
/**
 * 列出一个群组里的所有成员
 *
 * <p>成员就是群组节点下面的子节点，
 * 所以「列出群组」实际上就是「列出子节点」
 */
public class ListGroup extends ConnectionWatcher {
    
  /**
   * 列出群组里的成员
   *
   * @param groupName 群组名
   * @throws KeeperException 与 ZooKeeper 通信失败
   * @throws InterruptedException 等待被中断
   */
  public void list(String groupName) throws KeeperException,
      InterruptedException {
    // 群组节点的完整路径
    String path = "/" + groupName;
    
    try {
      // 群组的成员就是它的子节点
      List<String> children = zk.getChildren(path, false);
      // 一个成员都没有，这个群组是空的
      if (children.isEmpty()) {
        System.out.printf("No members in group %s\n", groupName);
        // 用非零退出码表示出错
        System.exit(1);
      }
      // 一个成员打印一行
      for (String child : children) {
        System.out.println(child);
      }
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
    ListGroup listGroup = new ListGroup();
    // 连上 ZooKeeper 并等连接就绪
    listGroup.connect(args[0]);
    // 列出群组成员
    listGroup.list(args[1]);
    listGroup.close();
  }
}
// ^^ ListGroup
