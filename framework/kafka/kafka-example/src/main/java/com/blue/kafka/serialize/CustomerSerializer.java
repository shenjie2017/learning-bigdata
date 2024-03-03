package com.blue.kafka.serialize;

import com.blue.kafka.entity.Customer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @Author: Jason
 * @E-mail: 1075850619@qq.com
 * @Date: create in 2019/1/2 11:32
 * @Modified by:
 * @Project: learning-kafka
 * @Package: com.blue.kafka.serialize
 * @Description:
 */
public class CustomerSerializer implements Serializer<Customer> {
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public byte[] serialize(String topic, Customer data) {
        try {
        byte[] serializeName = null;
        int byteSize = 0;
        if (null == data) {
            return null;
        } else {
            if (null != data.getName()) {

                    serializeName = data.getName().getBytes("UTF-8");
                    byteSize = serializeName.length;
            } else {
                serializeName = new byte[0];
                byteSize = serializeName.length;
            }
        }
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + byteSize);
        buffer.putInt(data.getId());
        buffer.putInt(byteSize);
        buffer.put(serializeName);
        return buffer.array();
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serialize Customer to byte[] ",e);
        }
    }

    public void close() {

    }
}
