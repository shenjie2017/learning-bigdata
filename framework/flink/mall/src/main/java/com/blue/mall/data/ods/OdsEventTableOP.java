package com.blue.mall.data.ods;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;


public class OdsEventTableOP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果key基本相同，需要设置并行度为1来开开发测试，因为水位线是参照并行度最低，
        // 如果设置为多个并行度，可能有的并行度并没有接收到数据，导致窗口无法关闭
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("create table ods_event( " +
                "id string, " +
                "url string, " +
                "`time` string, " +
                "ts bigint, " + //事件过程时间
                "time_ltz AS TO_TIMESTAMP_LTZ(ts, 3), " + //转换成时间戳
//                "user_action_time AS PROCTIME() "+
                "WATERMARK FOR time_ltz AS time_ltz - INTERVAL '10' SECOND " + //取最近10秒到数据
                ") with ( " +
                "'connector'='kafka', " +
                "'topic'='mall_click_event', " +
                "'properties.group.id'='data_group', " +
                "'properties.bootstrap.servers'='localhost:9092', " +
                "'scan.startup.mode'='latest-offset', " +
                "'value.format'='json' ) ");


//        tEnv.executeSql("select * from ods_event").print();
        Table t1 = tEnv.from("ods_event");
        Table t2 = t1.select($("id"), $("url"), $("time"), $("ts"), $("time_ltz"));
//        tEnv.toChangelogStream(t2).print("t2");

        Table t3 = t1
                .filter($("id").isNotEqual("joe"))
                .window(Tumble.over(lit(10).seconds()).on($("time_ltz")).as("w"))
                .groupBy($("w"),$("id"))
                .select($("w").start(),$("w").end(),$("id"),$("time").count().as("cnt"));
        tEnv.toChangelogStream(t3).print("t3");

//        Table t4 = t1.window(Over.partitionBy($("id"), $("url"))
//                        .orderBy($("time_ltz"))
//                        .preceding(UNBOUNDED_RANGE)
//                        .following(CURRENT_RANGE).as("w"))
//                .select($("id"), $("url"), $("time"), $("ts"), $("time").count().over($("w")));
//        tEnv.toChangelogStream(t4).print("t4");

        env.execute(" ods event table api op");
    }
}
