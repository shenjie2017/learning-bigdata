package com.blue.flink.rt.transfrom;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatmapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> elemDS = env.fromElements("hello jason", "my name is jason", "welcome to chengdu", "he is boy");

        SingleOutputStreamOperator<Tuple2<String,Integer>> wordDS = elemDS.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
                for (String s : value.split(" ")) {
                    out.collect(Tuple2.of(s,1));
                }
            }
        });

        wordDS.keyBy(0).sum(1).print();

        env.execute("FlatmapDemo");
    }
}
