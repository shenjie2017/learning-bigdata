package com.blue.flink.rt.combine;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elem1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<String> elem2 = env.fromElements("a","b","c");

        elem1.connect(elem2).process(new CoProcessFunction<Integer, String, String>() {
            @Override
            public void processElement1(Integer value, CoProcessFunction<Integer, String, String>.Context ctx, Collector<String> out) throws Exception {
                // System.out.println("processElement1--->"+value);
                out.collect("processElement1--->"+value);
            }

            @Override
            public void processElement2(String value, CoProcessFunction<Integer, String, String>.Context ctx, Collector<String> out) throws Exception {
                // System.out.println("processElement2--->"+value);
                out.collect("processElement2--->"+value);
            }
        }).print();

        env.execute(ConnectDemo.class.getName());
    }
}
