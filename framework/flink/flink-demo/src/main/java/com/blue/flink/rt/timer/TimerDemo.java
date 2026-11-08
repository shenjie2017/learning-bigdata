package com.blue.flink.rt.timer;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TimerDemo {
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

        SingleOutputStreamOperator<String> processDS = ksDS.process(new KeyedProcessFunction<String, Tuple3<String, Integer, Integer>, String>() {
            @Override
            public void processElement(Tuple3<String, Integer, Integer> value, KeyedProcessFunction<String, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                long watermark = ctx.timerService().currentWatermark();
                Long currentEventTs = ctx.timestamp();
                long currentProcessingTs = ctx.timerService().currentProcessingTime();
                //事件时间出发
                //long timer = currentEventTs+5000L;
                //ctx.timerService().registerEventTimeTimer(timer);
                //处理时间触发
                long timer = currentProcessingTs+5000L;
                ctx.timerService().registerProcessingTimeTimer(timer);
                System.out.println("create timer，currentEventTs:"+currentEventTs+",currentProcessingTs:"+currentProcessingTs+",watermark:"+watermark+",定时器时间:" + timer);
                System.out.println("process element:" + value);
                out.collect(value.toString());
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple3<String, Integer, Integer>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                long watermark = ctx.timerService().currentWatermark();
                Long currentEventTs = ctx.timestamp();
                long currentProcessingTs = ctx.timerService().currentProcessingTime();
                System.out.println("触发定时器，currentEventTs:"+currentEventTs+",currentProcessingTs:"+currentProcessingTs+",watermark:"+watermark+",定时器时间：" + timestamp);
                super.onTimer(timestamp, ctx, out);
            }
        });

        env.execute(TimerDemo.class.getName());
    }
}
