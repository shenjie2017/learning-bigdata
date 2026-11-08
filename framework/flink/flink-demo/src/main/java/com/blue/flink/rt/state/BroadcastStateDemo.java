package com.blue.flink.rt.state;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> orders = env.socketTextStream("127.0.0.1", 8888)
                .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(String value) throws Exception {
                        String id = value.split(",")[0];
                        Integer amount = Integer.parseInt(value.split(",")[1]);
                        Integer ts = Integer.parseInt(value.split(",")[2]);
                        return Tuple3.of(id, amount, ts);
                    }
                });

        SingleOutputStreamOperator<Tuple2<String, String>> users = env.socketTextStream("127.0.0.1", 7777)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        String id = value.split(",")[0];
                        String name = value.split(",")[1];
                        return Tuple2.of(id, name);
                    }
                });

        MapStateDescriptor<String, String> broadcastState = new MapStateDescriptor<>("broadcastState", Types.STRING, Types.STRING);
        BroadcastStream<Tuple2<String, String>> userBDS = users.broadcast(broadcastState);

        orders.connect(userBDS).process(new BroadcastProcessFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>, String>() {
            @Override
            public void processElement(Tuple3<String, Integer, Integer> value,
                                       BroadcastProcessFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>, String>.ReadOnlyContext ctx,
                                       Collector<String> out) throws Exception {
                String name = ctx.getBroadcastState(broadcastState).get(value.f0);
                if(name==null){
                    name = "unknown";
                }

                System.out.println("key:"+value.f0+",name:"+name+",value:"+value);

            }

            @Override
            public void processBroadcastElement(Tuple2<String, String> value,
                                                BroadcastProcessFunction<Tuple3<String, Integer, Integer>,
                                                        Tuple2<String, String>, String>.Context ctx,
                                                Collector<String> out) throws Exception {
                ctx.getBroadcastState(broadcastState).put(value.f0, value.f1);
                System.out.println("update broadcast state:"+value);
            }
        });

        env.execute();
    }
}
