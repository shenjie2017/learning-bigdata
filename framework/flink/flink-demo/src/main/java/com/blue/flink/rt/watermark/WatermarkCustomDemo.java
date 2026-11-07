package com.blue.flink.rt.watermark;

import com.blue.flink.rt.bean.Event;
import com.blue.flink.rt.source.MockSingleUserEventSource;
import com.blue.flink.util.JsonUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class WatermarkCustomDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.addSource(new MockSingleUserEventSource.CustomSourceFunction());
        env.getConfig().setAutoWatermarkInterval(1000L);
        SingleOutputStreamOperator<Event> eventDS = source.map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        return JsonUtil.fromJsonString(value,Event.class);
                    }
                })
                .map(new MapFunction<Event, Event>() {
                    @Override
                    public Event map(Event value) throws Exception {
                        System.out.println("mock data:"+new Timestamp(System.currentTimeMillis()).toString()+","+value);
                        return value;
                    }
                });

        SingleOutputStreamOperator<Event> watermarkDS = eventDS.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forGenerator(
                /**
                 * 自定义watermark生成器
                 */
                new WatermarkGeneratorSupplier<Event>() {
            @Override
            public WatermarkGenerator<Event> createWatermarkGenerator(Context context) {
                //自定义水位生成器逻辑
                return new CustomWatermarkGenerator<Event>(3000L);
            }
        }).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //滚动窗口，10秒滚动一次
        WindowedStream<Event, String, TimeWindow> eventWS = watermarkDS.keyBy(Event::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // 窗口关闭才会执行，输入和输出值类型可以不一致
        eventWS.process(new ProcessWindowFunction<Event, Integer, String, TimeWindow>() {
            @Override
            public void process(String key, ProcessWindowFunction<Event, Integer, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<Integer> out) throws Exception {
                String startTS = new Timestamp(context.window().getStart()).toString();
                String endTS = new Timestamp(context.window().getEnd()).toString();
                System.out.println("窗口key:"+key+"[记录数："+elements.spliterator().estimateSize()+","+startTS+","+endTS+"]"+JsonUtil.toJsonString(elements));
            }
        });

        env.execute(WatermarkCustomDemo.class.getName());
    }

}
