package com.blue.kafka.deserialize;

import com.blue.kafka.entity.Customer;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Reader;
import java.net.DatagramPacket;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @Author: Jason
 * @E-mail: 1075850619@qq.com
 * @Date: create in 2019/1/2 16:02
 * @Modified by:
 * @Project: learning-kafka
 * @Package: com.blue.kafka.deserialize
 * @Description:
 */
public class CustomerDeserializer implements Deserializer<Customer> {
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public Customer deserialize(String topic, byte[] data) {
        if (null == data || data.length < 8) {
            return null;
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        int customerId = buffer.getInt();
        int length = buffer.getInt();
        byte[] dest = new byte[length];
        buffer.get(dest);
        String customerName = new String(dest);
        return new Customer(customerId, customerName);
    }

    public void close() {

    }
}
