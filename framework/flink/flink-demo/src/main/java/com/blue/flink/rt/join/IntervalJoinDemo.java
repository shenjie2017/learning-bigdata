package com.blue.flink.rt.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<Tuple3<String, String, Integer>, String> ksDS1 = env.fromElements(
                        Tuple3.of("a", "zhangsan", 1),
                        Tuple3.of("a", "zs", 11),
                        Tuple3.of("b", "lisi", 3),
                        Tuple3.of("c", "wangwu", 8))
                .assignTimestampsAndWatermarks(
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

        KeyedStream<Tuple3<String, Integer, Integer>, String> ksDS2 = env.fromElements(
                        Tuple3.of("a", 100, 1),
                        Tuple3.of("a", 200, 3),
                        Tuple3.of("a", 200, 9),
                        Tuple3.of("b", 50, 4),
                        Tuple3.of("c", 160, 10))
                .assignTimestampsAndWatermarks(
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

        ksDS1.intervalJoin(ksDS2).between(Time.seconds(-2),Time.seconds(2))
                .process(new ProcessJoinFunction<Tuple3<String, String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Integer> left, Tuple3<String, Integer, Integer> right,
                                               ProcessJoinFunction<Tuple3<String, String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx,
                                               Collector<String> out) throws Exception {
                        System.out.println(left + "--------" + right);
                    }
                });

        env.execute(IntervalJoinDemo.class.getName());
    }
}
