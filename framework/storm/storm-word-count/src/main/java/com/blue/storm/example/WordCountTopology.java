package com.blue.storm.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 11:06 2018/5/21
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

public class WordCountTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        //1.准备TopologyBuilder
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new MySpout(),2);
        builder.setBolt("split",new MySplitBolt(),2).shuffleGrouping("spout");
        builder.setBolt("count",new MyCountBolt(),4).fieldsGrouping("split",new Fields("word"));

        //2.创建Config,用来指定Topology需要的worker数量
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(3);

        //3.提交任务    --本地模式和集群模式
        if (args!=null && args.length>0) {
            //集群模式
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }else {
            //本地模式
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("MyWordCount", config, builder.createTopology());
        }

    }

    //接收外部数据源，发送给bolt处理
    private static class MySpout extends BaseRichSpout {
        SpoutOutputCollector collector;

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        public void nextTuple() {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            collector.emit(new Values("i am jason i love xx") );
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("index001"));
        }
    }

    private static class MySplitBolt extends BaseRichBolt {
        OutputCollector collector;

        //初始化方法
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        public void execute(Tuple input) {
//            String line = input.getString(0);
            String line = (String)input.getValueByField(new Fields("index001").get(0));
            String[] words = line.split(" ");
            for(String word:words){
                collector.emit(new Values(word,1));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            //字段对应的下标，word的下标是0，num的下标是1,用于下一个Tuple取第几个value,也可以直接用数字下标获取
            declarer.declare(new Fields("word","num"));
        }
    }

    private static class MyCountBolt  extends BaseRichBolt {
        OutputCollector collector;
        Map<String,Integer> map;

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            this.map = new HashMap<String, Integer>();
        }

        public void execute(Tuple input) {
//            String word = input.getString(0);
            String word = (String)input.getValueByField(new Fields("word").get(0));
//            Integer num = input.getInteger(1);
            Integer num = (Integer) input.getValueByField(new Fields("num").get(0));

            if(map.containsKey(word)){
                map.put(word,map.get(word)+1);
            }else{
                map.put(word,1);
            }

            System.out.println("thread name:"+Thread.currentThread().getName()+"\t"+"word:"+word +"\t"+"count:"+map.get(word));
//            System.out.println("time:"+System.currentTimeMillis() +"\t" + "word:"+word+"\t"+"count:"+map.get(word));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }
}
