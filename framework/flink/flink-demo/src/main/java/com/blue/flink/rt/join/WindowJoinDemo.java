package com.blue.flink.rt.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static org.apache.flink.api.common.eventtime.WatermarkStrategy.forMonotonousTimestamps;

public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> ds1 = env.fromElements(
                        Tuple3.of("a", "zhangsan", 1),
                        Tuple3.of("a", "zs", 11),
                        Tuple3.of("b", "lisi", 3),
                        Tuple3.of("c", "wangwu", 8))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Integer>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Integer> element, long recordTimestamp) {
                                        return element.f2 * 1000L;
                                    }
                                }));

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.fromElements(
                        Tuple3.of("a", 100, 1),
                        Tuple3.of("a", 200, 3),
                        Tuple3.of("a", 200, 9),
                        Tuple3.of("b", 50, 4),
                        Tuple3.of("c", 160, 10))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Integer>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Integer, Integer> element, long recordTimestamp) {
                                        return element.f2 * 1000L;
                                    }
                                }));

        ds1.join(ds2).where(new KeySelector<Tuple3<String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                return value.f0;
            }
        }).equalTo(new KeySelector<Tuple3<String, Integer, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                return value.f0;
            }
        })
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .apply(new JoinFunction<Tuple3<String, String, Integer>, Tuple3<String, Integer, Integer>, Object>() {
            @Override
            public Object join(Tuple3<String, String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                System.out.println(first + "--------" + second);
                return null;
            }
        });

        env.execute(WindowJoinDemo.class.getName());
    }
}
