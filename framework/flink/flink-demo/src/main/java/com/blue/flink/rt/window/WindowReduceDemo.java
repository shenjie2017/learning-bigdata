package com.blue.flink.rt.window;

import com.blue.flink.rt.bean.Event;
import com.blue.flink.rt.source.MockEventSource;
import com.blue.flink.util.JsonUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

public class WindowReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.addSource(new MockEventSource.CustomSourceFunction());

        SingleOutputStreamOperator<Event> eventDS = source.map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        return JsonUtil.fromJsonString(value, Event.class);
                    }
                })
                .filter(new FilterFunction<Event>() {

                    @Override
                    public boolean filter(Event value) throws Exception {
                        return Arrays.asList("zhangsan", "lisi").contains(value.getId());
                    }
                }).map(new MapFunction<Event, Event>() {
                    @Override
                    public Event map(Event value) throws Exception {
                        System.out.println("mock data:" + value);
                        return value;
                    }
                });

        //滚动窗口，10秒滚动一次
        WindowedStream<Event, String, TimeWindow> eventWS = eventDS.keyBy(Event::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //reduce在窗口未关闭时也会执行,输入和输出类型值需要相同
        eventWS.reduce(new ReduceFunction<Event>() {

            @Override
            public Event reduce(Event value1, Event value2) throws Exception {
                System.out.println("reduce:" + value1 + "," + value2);
                return value2;
            }
        });

        env.execute(WindowReduceDemo.class.getName());
    }
}
