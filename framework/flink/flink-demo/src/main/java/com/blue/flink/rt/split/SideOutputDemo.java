package com.blue.flink.rt.split;

import com.blue.flink.rt.bean.Event;
import com.blue.flink.rt.partition.PartitionDemo;
import com.blue.flink.rt.source.MockEventSource;
import com.blue.flink.util.JsonUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputDemo {
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

        OutputTag<Event> warningTag = new OutputTag<>("warning", Types.POJO(Event.class));
        OutputTag<Event> helpTag = new OutputTag<>("help", Types.POJO(Event.class));
        OutputTag<Event> loginTag = new OutputTag<>("login", Types.POJO(Event.class));
        SingleOutputStreamOperator<Event> processDS = eventDS.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                if ("/warning".equals(value.getUrl())) {
                    ctx.output(warningTag, value);
                } else if ("/help".equals(value.getUrl())) {
                    ctx.output(helpTag, value);
                } else if ("/login".equals(value.getUrl())) {
                    ctx.output(loginTag, value);
                } else {
                    out.collect(value);
                }
            }
        });

        processDS.print("主流");
        processDS.getSideOutput(warningTag).print("warning");
        processDS.getSideOutput(helpTag).print("help");
        processDS.getSideOutput(loginTag).print("login");

        env.execute(PartitionDemo.class.getName());
    }
}
