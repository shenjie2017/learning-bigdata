package com.blue.flink.rt.combine;

import com.blue.flink.rt.aggreagte.KeyByDemo;
import com.blue.flink.rt.bean.Event;
import com.blue.flink.rt.source.MockEventSource;
import com.blue.flink.util.JsonUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elem1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<Integer> elem2 = env.fromElements(5, 6, 7, 8);

        elem1.union(elem2).print();

        env.execute(KeyByDemo.class.getName());
    }
}