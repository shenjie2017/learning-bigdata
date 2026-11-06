package com.blue.flink.rt.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> fileDS = env.readTextFile("/opt/apache/flink-1.15.0/LICENSE");

        fileDS.print();

        env.execute(FileSourceDemo.class.getClass().getName());
    }
}
