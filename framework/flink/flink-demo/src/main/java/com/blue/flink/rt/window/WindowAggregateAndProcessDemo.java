package com.blue.flink.rt.window;

import com.blue.flink.rt.bean.Event;
import com.blue.flink.rt.source.MockEventSource;
import com.blue.flink.util.JsonUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;

public class WindowAggregateAndProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.addSource(new MockEventSource.CustomSourceFunction());

        SingleOutputStreamOperator<Event> eventDS = source.map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        return JsonUtil.fromJsonString(value,Event.class);
                    }
                })
                .filter(new FilterFunction<Event>() {

                    @Override
                    public boolean filter(Event value) throws Exception {
                        return Arrays.asList("zhangsan").contains(value.getId());
                    }
                }).map(new MapFunction<Event, Event>() {
                    @Override
                    public Event map(Event value) throws Exception {
                        System.out.println("mock data:"+value);
                        return value;
                    }
                });

        //滚动窗口，10秒滚动一次
        WindowedStream<Event, String, TimeWindow> eventWS = eventDS.keyBy(Event::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // 输入和输出可以类型不一致
        eventWS.aggregate(new AggregateFunction<Event, Integer, Event>() {
                              Event event = null;
                              @Override
                              public Integer createAccumulator() {
                                  System.out.println("createAccumulator");
                                  return 0;
                              }

                              @Override
                              public Integer add(Event value, Integer accumulator) {
                                  if(null == event){
                                      event = value;
                                  }
                                  System.out.println("add:" + value + ",accumulator:" + accumulator);
                                  return accumulator + 1;
                              }

                              @Override
                              public Event getResult(Integer accumulator) {
                                  System.out.println("getResult:" + accumulator);
                                  return event;
                              }

                              @Override
                              public Integer merge(Integer a, Integer b) {
                                  System.out.println("merge:" + a + "," + b);
                                  return a + b;
                              }
                          },
                new ProcessWindowFunction<Event, Event, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<Event, Event, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<Event> out) throws Exception {
                        String startTS = new Timestamp(context.window().getStart()).toString();
                        String endTS = new Timestamp(context.window().getEnd()).toString();
                        System.out.println("窗口key:"+key+"["+startTS+","+endTS+"]"+JsonUtil.toJsonString(elements));
                    }
                });

        env.execute(WindowAggregateDemo.class.getSimpleName());
    }
}
