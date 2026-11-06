package com.blue.flink.rt.functions;

import com.blue.flink.rt.bean.Event;
import com.blue.flink.rt.source.MockEventSource;
import com.blue.flink.util.JsonUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.addSource(new MockEventSource.CustomSourceFunction());

        SingleOutputStreamOperator<Event> eventDS = source.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                return JsonUtil.fromJsonString(value,Event.class);
            }
        });

        SingleOutputStreamOperator<Event> result = eventDS.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                if ("warning".equals(value.getUrl())) {
                    return true;
                }
                return false;
            }
        });

        result.print();

        env.execute(FilterDemo.class.getName());
    }
}
