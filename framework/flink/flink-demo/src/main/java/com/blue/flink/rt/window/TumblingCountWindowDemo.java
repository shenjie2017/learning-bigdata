package com.blue.flink.rt.window;

import com.blue.flink.rt.bean.Event;
import com.blue.flink.rt.source.MockEventSource;
import com.blue.flink.util.JsonUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Time;
import java.sql.Timestamp;

public class TumblingCountWindowDemo {
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

        //滚动窗口，5条滚动一次
        WindowedStream<Event, String, GlobalWindow> eventWS = eventDS.keyBy(Event::getId)
                .countWindow(5);

        eventWS.process(new ProcessWindowFunction<Event, String, String, GlobalWindow>() {
            @Override
            public void process(String key, ProcessWindowFunction<Event, String, String, GlobalWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                System.out.println("窗口key:"+key+"[size:"+elements.spliterator().estimateSize()+"]"+JsonUtil.toJsonString(elements));
            }
        });

        env.execute(TumblingCountWindowDemo.class.getName());
    }
}
