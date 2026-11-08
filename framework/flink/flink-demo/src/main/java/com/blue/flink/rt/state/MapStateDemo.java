package com.blue.flink.rt.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class MapStateDemo {
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

            MapState<String,Integer> mapState;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String,Integer>("mapState", String.class,Integer.class));
            }

            @Override
            public void processElement(Tuple3<String, Integer, Integer> value, KeyedProcessFunction<String, Tuple3<String, Integer, Integer>, Object>.Context ctx, Collector<Object> out) throws Exception {
                int currCnt = null == mapState.get("cnt") ? 1 : mapState.get("cnt") + 1;
                int sumAmount = null == mapState.get("sum") ? value.f1 : mapState.get("sum") + value.f1;
                mapState.put("cnt", currCnt);
                mapState.put("sum", sumAmount);

                System.out.println("key:"+value.f0 + ",acc sum amount:" + sumAmount + ",cnt:" + currCnt + ",avg:"+sumAmount/currCnt);
            }
        });

        env.execute();
    }
}
