package com.blue.flink.rt.functions;

import com.blue.flink.rt.bean.Event;
import com.blue.flink.rt.source.MockEventSource;
import com.blue.flink.util.JsonUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class RichMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> source = env.addSource(new MockEventSource.CustomSourceFunction());

        SingleOutputStreamOperator<Event> eventDS = source.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                return JsonUtil.fromJsonString(value,Event.class);
            }
        });

        SingleOutputStreamOperator<Event> filterDS = eventDS.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                if ("warning".equals(value.getUrl())) {
                    return true;
                }
                return false;
            }
        });

        SingleOutputStreamOperator<Tuple2<Event, Integer>> mapDS = filterDS.keyBy(event ->event.id).
                map(new RichMapFunction<Event, Tuple2<Event, Integer>>() {

            Map<String, Integer> map = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("call Rich function open");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("call Rich function close");
            }

            @Override
            public Tuple2<Event, Integer> map(Event value) throws Exception {
                if (null == map.get(value.id)) {
                    map.put(value.id, 0);
                }
                Integer accCnt = map.get(value.id) + 1;
                map.put(value.id, accCnt);
                return Tuple2.of(value, accCnt);
            }
        });

        mapDS.print();
        
        env.execute(RichMapDemo.class.getName());
    }
}
