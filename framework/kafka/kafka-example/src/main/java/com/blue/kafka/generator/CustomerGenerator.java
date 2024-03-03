package com.blue.kafka.generator;

import com.blue.kafka.entity.Customer;

import java.util.UUID;

/**
 * @Author: Jason
 * @E-mail: 1075850619@qq.com
 * @Date: create in 2019/1/2 10:15
 * @Modified by:
 * @Project: learning-kafka
 * @Package: com.blue.kafka.generator
 * @Description:
 */
public class CustomerGenerator {
    private static int id = 0;

    public static Customer getNext(){
        return new Customer(id++, UUID.randomUUID().toString());
    }
}
