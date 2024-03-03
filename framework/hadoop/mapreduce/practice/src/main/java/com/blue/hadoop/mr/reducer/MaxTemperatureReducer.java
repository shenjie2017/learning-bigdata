package com.blue.hadoop.mr.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @Author: Jason
 * @E-mail: 1075850619@qq.com
 * @Date: create in 2018/9/20 15:15
 * @Modified by:
 * @Project: learning-hadoop
 * @Package: com.blue.hadoop.mr.reducer
 * @Description:
 */
public class MaxTemperatureReducer extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double max = Integer.MIN_VALUE;
        for(DoubleWritable value:values){
            max = Math.max(max,value.get());
        }
        context.write(key,new DoubleWritable(max));
    }
}
