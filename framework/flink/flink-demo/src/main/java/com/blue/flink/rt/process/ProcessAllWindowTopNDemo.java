package com.blue.flink.rt.process;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

public class ProcessAllWindowTopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<Tuple3<String, Integer, Integer>, String> ksDS = env.socketTextStream("127.0.0.1", 8888)
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

        ksDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .process(new ProcessAllWindowFunction<Tuple3<String, Integer, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Tuple3<String, Integer, Integer>, String, TimeWindow>.Context context, Iterable<Tuple3<String, Integer, Integer>> elements, Collector<String> out) throws Exception {
                        Map<String,Integer> idCntMap = new HashMap<String,Integer>();

                        for (Tuple3<String, Integer, Integer> element : elements) {
                            if(idCntMap.containsKey(element.f0)) {
                                idCntMap.put(element.f0, idCntMap.get(element.f0) + 1);
                            }else  {
                                idCntMap.put(element.f0, 1);
                            }
                        }

                        List<Tuple2<String,Integer>> idCntList = new ArrayList<Tuple2<String,Integer>>();

                        for (String key : idCntMap.keySet()) {
                            idCntList.add(Tuple2.of(key,idCntMap.get(key)));
                        }

                        idCntList.sort(new Comparator<Tuple2<String, Integer>>() {
                            @Override
                            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                                return o2.f1 - o1.f1;
                            }
                        });

                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();
                        System.out.println("[startTs:"+startTs+",endTs:"+endTs+"]count:"+idCntList);
                    }
                });

        env.execute();
    }
}
