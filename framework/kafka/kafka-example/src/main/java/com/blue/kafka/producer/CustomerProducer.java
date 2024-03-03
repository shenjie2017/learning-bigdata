package com.blue.kafka.producer;

import com.blue.kafka.entity.Customer;
import com.blue.kafka.generator.CustomerGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Author: Jason
 * @E-mail: 1075850619@qq.com
 * @Date: create in 2019/1/2 10:05
 * @Modified by:
 * @Project: learning-kafka
 * @Package: com.blue.kafka.avro
 * @Description:
 */
public class CustomerProducer {

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
       props.put("value.serializer", "com.blue.kafka.serialize.CustomerSerializer");

        String topic = "customerContacts";
        Producer<String, Customer> producer = new KafkaProducer<String, Customer>(props);

        for (int i = 0; i < 20; i++) {
            Customer customer = CustomerGenerator.getNext();
            System.out.println("Generated customer " + customer);

            ProducerRecord<String, Customer> record = new ProducerRecord<String, Customer>(topic, String.valueOf(customer.getId()), customer);
            producer.send(record);
        }
        producer.close();
    }
}
