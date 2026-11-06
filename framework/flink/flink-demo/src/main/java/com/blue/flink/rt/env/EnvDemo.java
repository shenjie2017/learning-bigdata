package com.blue.flink.rt.env;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Integer> src = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        src.print();

        env.execute("env demo");
    }
}
