package com.blue.flink.rt.source;

import com.blue.flink.rt.bean.Event;
import com.blue.flink.util.JsonUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class MockSingleUserEventSource {
    public static class CustomSourceFunction implements SourceFunction<String> {
        boolean running=true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            String[] names = {"zhangsan"};
            String[] urls = {"/home","/prod?id=3","/prod?id=5","/help","/login","/warning"};
            Random rand = new Random();
            while (running){
                Event event = new Event(names[rand.nextInt(names.length)], urls[rand.nextInt(urls.length)], System.currentTimeMillis());
                String jsonStr = JsonUtil.toJsonString(event);
                if(rand.nextInt(3)==0){
                    // mock数据乱序
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Thread.sleep(rand.nextInt(10000)+2000);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            ctx.collect(jsonStr);
                        }
                    }).start();

                }else {
                    ctx.collect(jsonStr);
                }
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running=false;
        }
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new CustomSourceFunction()).print();

        env.execute("stream rand click data");
    }
}
