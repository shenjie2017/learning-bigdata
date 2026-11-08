package com.blue.flink.rt.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ReducingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<Tuple3<String, Integer, Integer>, String> ks = env.socketTextStream("127.0.0.1", 8888)
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
                                }))
                .keyBy(new KeySelector<Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public String getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                        return value.f0;
                    }
                });

        ks.process(new KeyedProcessFunction<String, Tuple3<String, Integer, Integer>, Object>() {

            ReducingState<Integer> reducingState;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("reducingState", new ReduceFunction<Integer>() {

                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1+value2;
                    }
                }, Types.INT));
            }

            @Override
            public void processElement(Tuple3<String, Integer, Integer> value, KeyedProcessFunction<String, Tuple3<String, Integer, Integer>, Object>.Context ctx, Collector<Object> out) throws Exception {
                reducingState.add(value.f1);

                System.out.println("key:"+value.f0 + ",acc sum amount:"+reducingState.get());
            }
        });

        env.execute();
    }
}
