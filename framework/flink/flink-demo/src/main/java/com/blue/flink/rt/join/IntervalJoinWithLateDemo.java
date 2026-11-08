package com.blue.flink.rt.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class IntervalJoinWithLateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*
        a,zhangsan,100
        a,zhangsan,1
        a,zs,11
        a,z,102
        a,z,100
        b,lisi,3
        c,wangwu,8
        a,zhang,6
         */
        KeyedStream<Tuple3<String, String, Integer>, String> ksDS1 = env.socketTextStream("127.0.0.1", 7777)
                .map(new MapFunction<String, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> map(String value) throws Exception {
                        String id = value.split(",")[0];
                        String name = value.split(",")[1];
                        Integer ts = Integer.parseInt(value.split(",")[2]);
                        return Tuple3.of(id, name, ts);
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Integer>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Integer> element, long recordTimestamp) {
                                        return element.f2 * 1000L;
                                    }
                                })).keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                        return value.f0;
                    }
                });


        /*
        a,100,1
        a,300,3
        a,900,9
        a,200,2
        a,9900,99
        b,40,4
        c,160,10
        a,400,20
         */
        KeyedStream<Tuple3<String, Integer, Integer>, String> ksDS2 = env.socketTextStream("127.0.0.1", 8888)
                .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(String value) throws Exception {
                        String id = value.split(",")[0];
                        Integer amount = Integer.parseInt(value.split(",")[1]);
                        Integer ts = Integer.parseInt(value.split(",")[2]);
                        return Tuple3.of(id, amount, ts);
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Integer>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Integer, Integer> element, long recordTimestamp) {
                                        return element.f2 * 1000L;
                                    }
                                })).keyBy(new KeySelector<Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public String getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                        return value.f0;
                    }
                });

        OutputTag<Tuple3<String, String, Integer>> leftLaterDS =
                new OutputTag<Tuple3<String, String, Integer>>("left-later", Types.TUPLE(Types.STRING, Types.STRING, Types.INT));
        OutputTag<Tuple3<String, Integer, Integer>> rightLaterDS =
                new OutputTag<Tuple3<String, Integer, Integer>>("right-later", Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        SingleOutputStreamOperator<String> processDS = ksDS1.intervalJoin(ksDS2)
                .between(Time.seconds(-2), Time.seconds(2))
                // 1.17.x才支持 sideOutputLeftLateData
                //.sideOutputLeftLateData(leftLaterDS)
                //.sideOutputRightLateData(rightLaterDS)
                .process(new ProcessJoinFunction<Tuple3<String, String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Integer> left, Tuple3<String, Integer, Integer> right,
                                               ProcessJoinFunction<Tuple3<String, String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx,
                                               Collector<String> out) throws Exception {
                        System.out.println(left + "--------" + right);
                    }
                });

        processDS.getSideOutput(leftLaterDS).printToErr("left-later");
        processDS.getSideOutput(rightLaterDS).printToErr("right-later");


        env.execute(IntervalJoinWithLateDemo.class.getName());
    }
}
