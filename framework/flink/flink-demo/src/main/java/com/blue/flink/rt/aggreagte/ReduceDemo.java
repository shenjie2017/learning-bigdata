package com.blue.flink.rt.aggreagte;

import com.blue.flink.rt.bean.Event;
import com.blue.flink.rt.source.MockEventSource;
import com.blue.flink.util.JsonUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.addSource(new MockEventSource.CustomSourceFunction());

        SingleOutputStreamOperator<Event> eventDS = source.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                return JsonUtil.fromJsonString(value,Event.class);
            }
        });

        eventDS.map(new MapFunction<Event, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Event value) throws Exception {
                System.out.println(value);
                return Tuple2.of(value.id,1);
            }
        }).keyBy(0).reduce(new ReduceFunction<Tuple2<String,Integer>>() {

            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0,value1.f1+value2.f1);
            }
        }).print();

        env.execute(KeyByDemo.class.getName());
    }
}
