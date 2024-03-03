package com.blue.zkdist;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 16:00 2017/12/21
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

public class DistributedServer {
    private final static String connectString = "192.168.163.111:2181,192.168.163.112:2181,192.168.163.113:2181";
    private final static int sessionTimeout = 2000;
    private final static String parentNode = "/servers";
    private ZooKeeper zk = null;

    /**
     * 获取创建zookeeper的客户端连接
     * @throws IOException
     */
    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "---" + watchedEvent.getPath());
                try {
                    //监听只会生效一次，除非再次监听
                    zk.getChildren("/",true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 利用zk连接注册服务器信息
     * @param hostname
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void registerServer(String hostname) throws KeeperException, InterruptedException {
        String create = zk.create(parentNode+"/server",hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + "is online.." + create);
    }

    /**
     * 业务功能
     * @param hostname
     * @throws InterruptedException
     */
    public void handlerBussiness(String hostname) throws InterruptedException {
        System.out.println(hostname + "start working......");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributedServer server = new DistributedServer();

        //获取zk连接
        server.getConnect();

        //利用zk连接注册服务器信息
        server.registerServer(args[0]);

        //启动业务功能
        server.handlerBussiness(args[0]);
    }
}
