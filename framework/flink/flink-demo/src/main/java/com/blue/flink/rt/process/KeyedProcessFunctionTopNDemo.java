package com.blue.flink.rt.process;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

public class KeyedProcessFunctionTopNDemo {
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

        SingleOutputStreamOperator<Tuple4<String, Integer, Long, Long>> aggregateDS = ks.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple3<String, Integer, Integer>, Integer, Integer>() {

                               @Override
                               public Integer createAccumulator() {
                                   return 0;
                               }

                               @Override
                               public Integer add(Tuple3<String, Integer, Integer> value, Integer accumulator) {
                                   return accumulator + 1;
                               }

                               @Override
                               public Integer getResult(Integer accumulator) {
                                   return accumulator;
                               }

                               @Override
                               public Integer merge(Integer a, Integer b) {
                                   return a + b;
                               }
                           },
                        new ProcessWindowFunction<Integer, Tuple4<String, Integer, Long, Long>, String, TimeWindow>() {

                            @Override
                            public void process(String key, ProcessWindowFunction<Integer,
                                                        Tuple4<String, Integer, Long, Long>, String, TimeWindow>.Context context,
                                                Iterable<Integer> elements,
                                                Collector<Tuple4<String, Integer, Long, Long>> out) throws Exception {
                                long winStartTs = context.window().getStart();
                                long winEndTs = context.window().getEnd();
                                String windowTs = winStartTs + "----" + winEndTs;
                                Tuple4<String, Integer, Long, Long> currentCnt = Tuple4.of(key, elements.iterator().next(), winStartTs, winEndTs);
                                out.collect(currentCnt);
                            }
                        });

        SingleOutputStreamOperator<List<Tuple4<String, Integer, Long, Long>>> processResultDS = aggregateDS.keyBy(new KeySelector<Tuple4<String, Integer, Long, Long>, Long>() {

            @Override
            public Long getKey(Tuple4<String, Integer, Long, Long> value) throws Exception {
                return value.f3;
            }
        }).process(new KeyedProcessFunction<Long, Tuple4<String, Integer, Long, Long>, List<Tuple4<String, Integer, Long, Long>>>() {
            final Map<Long, List<Tuple4<String, Integer, Long, Long>>> windowMap = new HashMap<>();

            @Override
            public void processElement(Tuple4<String, Integer, Long, Long> value,
                                       KeyedProcessFunction<Long, Tuple4<String, Integer, Long, Long>,
                                               List<Tuple4<String, Integer, Long, Long>>>.Context ctx, Collector<List<Tuple4<String, Integer, Long, Long>>> out) throws Exception {
                Long key = ctx.getCurrentKey();
                if (windowMap.containsKey(key)) {
                    windowMap.get(key).add(value);
                } else {
                    List<Tuple4<String, Integer, Long, Long>> list = new ArrayList<>();
                    list.add(value);
                    windowMap.put(key, list);
                }

                ctx.timerService().registerEventTimeTimer(value.f3 + 1);
            }

            @Override
            public void onTimer(long timestamp,
                                KeyedProcessFunction<Long, Tuple4<String, Integer, Long, Long>, List<Tuple4<String, Integer, Long, Long>>>.OnTimerContext ctx,
                                Collector<List<Tuple4<String, Integer, Long, Long>>> out) throws Exception {
                Long key = ctx.getCurrentKey();
                List<Tuple4<String, Integer, Long, Long>> list = windowMap.get(key);
                list.sort(new Comparator<Tuple4<String, Integer, Long, Long>>() {
                    @Override
                    public int compare(Tuple4<String, Integer, Long, Long> o1, Tuple4<String, Integer, Long, Long> o2) {
                        return o2.f1 - o1.f1;
                    }
                });
                out.collect(windowMap.get(key));
                super.onTimer(timestamp, ctx, out);
            }
        });

        processResultDS.print();

        env.execute();
    }
}
