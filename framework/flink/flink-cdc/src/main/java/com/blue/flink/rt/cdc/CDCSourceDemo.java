package com.blue.flink.rt.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

public class CDCSourceDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        env.enableCheckpointing(5000L);
        // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确认 checkpoints 之间的时间会进行 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(30000);
        // 允许两个连续的 checkpoint 错误
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
        // 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 开启实验性的 unaligned checkpoints
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("bigdata")
                .password("123456")
                .databaseList("bigdata")
                .tableList("bigdata.event")
                .connectTimeout(Duration.ofSeconds(10))
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        Properties producerProps = new Properties();
        producerProps.setProperty("transaction.timeout.ms", "600000");
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("event_cdc")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("kafka-event-")
                .setKafkaProducerConfig(producerProps)
                .build();

        env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "cdc-mysql-source").sinkTo(kafkaSink);
        

        env.execute(CDCSourceDemo.class.getName());
    }
}
