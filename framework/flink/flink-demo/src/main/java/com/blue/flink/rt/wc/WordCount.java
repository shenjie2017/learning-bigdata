package com.blue.flink.rt.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT,"8081");
        conf.setString(RestOptions.BIND_ADDRESS,"0.0.0.0");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(3);
        DataStreamSource<String> localhost = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = localhost.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                System.out.println("recive:"+line);
                String[] words = line.toLowerCase().split("\\W+");
                for (String word : words) {
                    if (!"".equals(word)) {
                        out.collect(Tuple2.of(word, 1));
                    }
                }
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1);
        result.print("result: ");
        env.execute("word count realtime execute");
    }
}
