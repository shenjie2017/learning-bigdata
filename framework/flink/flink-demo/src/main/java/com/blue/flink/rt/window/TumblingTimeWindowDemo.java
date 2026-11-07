package com.blue.flink.rt.window;

import com.blue.flink.rt.bean.Event;
import com.blue.flink.rt.source.MockEventSource;
import com.blue.flink.util.JsonUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class TumblingTimeWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.addSource(new MockEventSource.CustomSourceFunction());

        SingleOutputStreamOperator<Event> eventDS = source.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                System.out.println("mock data:"+value);
                return JsonUtil.fromJsonString(value,Event.class);
            }
        });

        //滚动窗口，10秒滚动一次
        WindowedStream<Event, String, TimeWindow> eventWS = eventDS.keyBy(Event::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        eventWS.process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
            @Override
            public void process(String key, ProcessWindowFunction<Event, String, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                String startTS = new Timestamp(context.window().getStart()).toString();
                String endTS = new Timestamp(context.window().getEnd()).toString();
                System.out.println("窗口key:"+key+"["+startTS+","+endTS+"]"+JsonUtil.toJsonString(elements));
            }
        });

        env.execute(TumblingTimeWindowDemo.class.getName());
    }
}
