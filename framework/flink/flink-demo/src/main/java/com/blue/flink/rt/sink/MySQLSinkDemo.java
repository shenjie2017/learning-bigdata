package com.blue.flink.rt.sink;

import com.blue.flink.rt.bean.Event;
import com.blue.flink.util.JsonUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setGroupId("kafka-event-study")
                .setTopics("kafka-event")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<Event> eventDS = kafkaDS.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                return JsonUtil.fromJsonString(value,Event.class);
            }
        });

        SinkFunction<Event> mysqlSink = JdbcSink.sink("insert into bigdata.event(id,url,ts,dt) values(?,?,?,?)",
                new JdbcStatementBuilder<Event>() {

                    @Override
                    public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                        preparedStatement.setString(1, event.getId());
                        preparedStatement.setString(2, event.getUrl());
                        preparedStatement.setLong(3, event.getTs());
                        preparedStatement.setString(4, event.getDt());
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(500)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://127.0.0.1:3306/bigdata")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );

        eventDS.addSink(mysqlSink);

        eventDS.print();

        env.execute(MySQLSinkDemo.class.getName());
    }
}
