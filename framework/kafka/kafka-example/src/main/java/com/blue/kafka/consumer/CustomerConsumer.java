package com.blue.kafka.consumer;

import com.blue.kafka.entity.Customer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * @Author: Jason
 * @E-mail: 1075850619@qq.com
 * @Date: create in 2019/1/2 16:05
 * @Modified by:
 * @Project: learning-kafka
 * @Package: com.blue.kafka.consumer
 * @Description:
 */
public class CustomerConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.blue.kafka.deserialize.CustomerDeserializer");
        props.put("group.id", "test_group");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("max.poll.records", 1000);

        String topic = "customerContacts";
        Consumer<String, Customer> consumer = new KafkaConsumer<String, Customer>(props);
        consumer.subscribe(Arrays.asList(topic));
        ConsumerRecords<String,Customer> records = consumer.poll(1000);

        for(ConsumerRecord<String,Customer> record:records){
            System.out.println(String.format("topic=%s, partition=%s, offset=%d, consumer=%s, customer=%s",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }

        consumer.close();
    }
}
