package com.blue.flink.rt.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class AggregatingStateDemo {
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

        ks.process(new KeyedProcessFunction<String, Tuple3<String, Integer, Integer>, Double>() {

            AggregatingState<Integer, Double> aggregatingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.aggregatingState = getRuntimeContext().getAggregatingState(
                        new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
                                "aggregatingState",
                                new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                    @Override
                                    public Tuple2<Integer, Integer> createAccumulator() {
                                        return Tuple2.of(0, 0);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                        return Tuple2.of(value + accumulator.f0, accumulator.f1 + 1);
                                    }

                                    @Override
                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                        return accumulator.f0 * 1D / accumulator.f1;
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                                    }
                                },
                                Types.TUPLE(Types.INT, Types.INT))
                );
            }

            @Override
            public void processElement(Tuple3<String, Integer, Integer> value, KeyedProcessFunction<String, Tuple3<String, Integer, Integer>, Double>.Context ctx, Collector<Double> out) throws Exception {
                this.aggregatingState.add(value.f1);
                Double avg = this.aggregatingState.get();
                System.out.println("key:" + value.f0 + ",avg:" + avg);
                out.collect(avg);
            }
        });

        env.execute();
    }
}
