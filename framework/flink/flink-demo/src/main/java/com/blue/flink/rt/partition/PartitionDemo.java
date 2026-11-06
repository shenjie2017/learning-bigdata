package com.blue.flink.rt.partition;

import com.blue.flink.rt.bean.Event;
import com.blue.flink.rt.source.MockEventSource;
import com.blue.flink.util.JsonUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionDemo {
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

        DataStream<Event> eventDataStream = eventDS.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                return Math.abs(key.hashCode()) % numPartitions;
            }
        }, new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.id;
            }
        });

        eventDataStream.print();

        env.execute(PartitionDemo.class.getName());
    }
}
