package com.blue.kafka.entity;

/**
 * @Author: Jason
 * @E-mail: 1075850619@qq.com
 * @Date: create in 2019/1/2 9:58
 * @Modified by:
 * @Project: learning-kafka
 * @Package: com.blue.kafka.entity
 * @Description:
 */
public class Customer {
    private int customerId;
    private String customerName;

    public Customer(int customerId, String customerName) {
        this.customerId = customerId;
        this.customerName = customerName;
    }

    public int getId() {
        return customerId;
    }

    public String getName() {
        return customerName;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "customerId=" + customerId +
                ", customerName='" + customerName + '\'' +
                '}';
    }
}
