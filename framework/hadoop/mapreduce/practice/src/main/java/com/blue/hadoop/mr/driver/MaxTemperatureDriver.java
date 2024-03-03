package com.blue.hadoop.mr.driver;

import com.blue.hadoop.mr.mapper.MaxTemperatureMapper;
import com.blue.hadoop.mr.reducer.MaxTemperatureReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author: Jason
 * @E-mail: 1075850619@qq.com
 * @Date: create in 2018/9/20 15:20
 * @Modified by:
 * @Project: learning-hadoop
 * @Package: com.blue.hadoop.mr
 * @Description:
 */
public class MaxTemperatureDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if(2!=args.length){
            System.out.printf("usage %s [generic option] <input> <output>\n",getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(),"max temperature");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureDriver(),args);
        System.exit(exitCode);
    }
}
