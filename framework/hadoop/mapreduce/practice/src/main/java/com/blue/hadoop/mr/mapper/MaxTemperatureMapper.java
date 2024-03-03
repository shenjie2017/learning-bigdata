package com.blue.hadoop.mr.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: Jason
 * @E-mail: 1075850619@qq.com
 * @Date: create in 2018/9/20 14:25
 * @Modified by:
 * @Project: learning-hadoop
 * @Package: com.blue.hadoop.mr.mapper
 * @Description: 计算当年最大温度mapper
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text,Text, DoubleWritable> {

    private String fieldSplit = ",";
    @Override
    protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(fieldSplit);

        context.write(new Text(fields[0]),new DoubleWritable(Double.valueOf(fields[1])));
    }
}
