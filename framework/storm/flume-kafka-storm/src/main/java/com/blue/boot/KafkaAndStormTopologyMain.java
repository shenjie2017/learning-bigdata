package com.blue.boot;

import com.blue.bolt.MyBolt1;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.topology.TopologyBuilder;

import java.util.HashMap;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 10:17 2018/5/29
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

public class KafkaAndStormTopologyMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        TopologyBuilder builder = new TopologyBuilder();
        SpoutConfig config = new SpoutConfig(new ZkHosts("192.168.163.111:2181"),"orderMq","/mykafka","kafkaSpout");
        builder.setSpout("kafkaSpout",new KafkaSpout(config),1);
        builder.setBolt("mybolt1",new MyBolt1(),1).shuffleGrouping("kafkaSpout");

        Config conf = new Config();
        //打印调试信息
        // conf.setDebug(true);
        if (args!=null && args.length>0) {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("storm2kafka", conf, builder.createTopology());
        }
    }
}
