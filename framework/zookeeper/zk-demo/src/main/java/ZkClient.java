import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 10:50 2017/12/21
 * @Modifide By:
 **/

//      ┏┛ ┻━━━━━┛ ┻┓
//      ┃　　　　　　 ┃
//      ┃　　　━　　　┃
//      ┃　┳┛　  ┗┳　┃
//      ┃　　　　　　 ┃
//      ┃　　　┻　　　┃
//      ┃　　　　　　 ┃
//      ┗━┓　　　┏━━━┛
//        ┃　　　┃   神兽保佑
//        ┃　　　┃   代码无BUG！
//        ┃　　　┗━━━━━━━━━┓
//        ┃　　　　　　　    ┣┓
//        ┃　　　　         ┏┛
//        ┗━┓ ┓ ┏━━━┳ ┓ ┏━┛
//          ┃ ┫ ┫   ┃ ┫ ┫
//          ┗━┻━┛   ┗━┻━┛

public class ZkClient {
    private final static String connectString = "192.168.163.111:2181,192.168.163.112:2181,192.168.163.113:2181";
    private final static int sessionTimeout = 2000;
    private  ZooKeeper zkClient = null;

    @Before
    /**
     * 初始化
     */
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "---" + watchedEvent.getPath());
                try {
                    //监听只会生效一次，除非再次监听
                    zkClient.getChildren("/",true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Test
    /**
     * 创建数据节点到zk中
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void testCreate() throws KeeperException, InterruptedException {
        //数据的增删改查
        String noteCreated = zkClient.create("/idea","hellozk".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    /**
     * 获取子节点
     */
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/",true);
        for(String child:children){
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    /**
     * 判断znode是否存在
     */
    public void testExist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/idea",false);
        System.out.println(null==stat ? "not exist":"exist");
    }

    @Test
    /**
     * 获取znote的数据
     */
    public void getData() throws KeeperException, InterruptedException, UnsupportedEncodingException {
        byte[] data = zkClient.getData("/idea",false,null);
        System.out.println(new String(data));
    }

    @Test
    /**
     * 删除znote的数据
     */
    public void deleteZnode() throws KeeperException, InterruptedException {
        zkClient.delete("/idea",-1);
    }

    @Test
    /**
     * 设置数据
     */
    public void setData() throws KeeperException, InterruptedException {
        zkClient.setData("/root_node","i love xx".getBytes(),-1);
    }


}
