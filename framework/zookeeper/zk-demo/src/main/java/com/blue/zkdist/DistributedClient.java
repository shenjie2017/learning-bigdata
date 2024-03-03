package com.blue.zkdist;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import sun.rmi.runtime.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 15:13 2017/12/26
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

public class DistributedClient {

    private final static String connectString = "192.168.163.111:2181,192.168.163.112:2181,192.168.163.113:2181";
    private final static int sessionTimeout = 2000;
    private final static String parentNode = "/servers";
    private ZooKeeper zk = null;

    //volatile表示每次使用时强制从内存读取，不从缓存读取，如果有更新，也强制更新到内存
    private volatile List<String> serverList = null;

    /**
     * 获取创建zookeeper的客户端连接
     * @throws IOException
     */
    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                try {
                    //重新更新服务器列表，并且注册了监听
                    getServerList();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 获取服务器信息列表
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void getServerList() throws KeeperException, InterruptedException {
        List<String> childern = zk.getChildren(parentNode,true);
        List<String> servers = new ArrayList<String>();
        for(String child:childern){
            byte[] data = zk.getData(parentNode+"/"+child,true,null);
            servers.add(new String(data));
        }
        //把servers赋值给成员变量serverList，提供给各业务现成调用
        serverList = servers;

        //打印服务器列表
        System.out.println(serverList.toString());
    }

    public void handlerBussiness() throws InterruptedException {
        System.out.println("client start working....");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributedClient client = new DistributedClient();
        //获取zk连接
        client.getConnect();

        //获取servers子节点信息,从中获取服务器信息列表
        client.getServerList();

        //业务线程启动
        client.handlerBussiness();

    }
}
