package com.blue.hadoop.mr.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author: Jason
 * @E-mail: 1075850619@qq.com
 * @Date: create in 2018/9/20 14:42
 * @Modified by:
 * @Project: learning-hadoop
 * @Package: com.blue.hadoop.mr.mapper
 * @Description:
 */
public class MaxTemperatureMapperTest {

    @Test
    public void processValidRecord() throws IOException {
        Text value = new Text("2018,27.5");
        new MapDriver<LongWritable,Text,Text,DoubleWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0),value)
                .withOutput(new Text("2018"),new DoubleWritable(27.5))
                .runTest();
    }
}
