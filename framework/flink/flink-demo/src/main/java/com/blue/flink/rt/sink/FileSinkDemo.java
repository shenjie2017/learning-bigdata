package com.blue.flink.rt.sink;

import com.blue.flink.rt.source.MockEventSource;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class FileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10000);
        DataStreamSource<String> sourceDS = env.addSource(new MockEventSource.CustomSourceFunction());

        FileSink<String> fileSink = FileSink.forRowFormat(new Path("/tmp/event"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("mock-event")
                        .withPartSuffix(".json").build())
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd", ZoneId.systemDefault()))
                .withBucketCheckInterval(Duration.ofSeconds(5).toMillis())
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.ofSeconds(60*60*1000))
                        .withMaxPartSize(new MemorySize(1024*1024*1024))
                        .build()
                )
                .build();

        sourceDS.sinkTo(fileSink);

        env.execute(FileSinkDemo.class.getName());
    }
}
