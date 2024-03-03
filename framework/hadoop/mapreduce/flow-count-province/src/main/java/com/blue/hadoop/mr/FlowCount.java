package com.blue.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 10:52 2018/1/17
 * @Modifide By:
 **/

//      ┏┛ ┻━━━━━┛ ┻┓
//      ┃　　　　　　 ┃
//      ┃　　　━　　　┃
//      ┃　┳┛　  ┗┳　┃
//      ┃　　　　　　 ┃
//      ┃　　　┻　　　┃
//      ┃　　　　　　 ┃
//      ┗━┓　　　┏━━━┛
//        ┃　　　┃   神兽保佑
//        ┃　　　┃   代码无BUG！
//        ┃　　　┗━━━━━━━━━┓
//        ┃　　　　　　　    ┣┓
//        ┃　　　　         ┏┛
//        ┗━┓ ┓ ┏━━━┳ ┓ ┏━┛
//          ┃ ┫ ┫   ┃ ┫ ┫
//          ┗━┻━┛   ┗━┻━┛

public class FlowCount {
    static class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");
            String phone = fields[1];
            long upFlow = Long.parseLong(fields[fields.length-3]);
            long dFlow = Long.parseLong(fields[fields.length-2]);
            context.write(new Text(phone),new FlowBean(upFlow,dFlow));
        }
    }

    static class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long upFlows = 0;
            long dFlows = 0;
            for(FlowBean flow:values){
                upFlows += flow.getUpFlow();
                dFlows += flow.getdFlow();
            }
            context.write(key,new FlowBean(key.toString(),upFlows,dFlows));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.map.memory.mb","2048");
        conf.set("mapreduce.reduce.memory.mb","2048");
        conf.set("mapreduce.map.java.opts","-Xmx2048m");
        conf.set("mapreduce.reduce.java.opts","-Xmx2048m");
        //构造yarn的任务
        Job job = Job.getInstance(conf);

        //设置jar包的位置
        job.setJarByClass(FlowCount.class);

        //设置map task处理类
        job.setMapperClass(FlowMapper.class);
        //设置reduce task处理类
        job.setReducerClass(FlowReducer.class);

        //设置map task的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //设置reduce task的输出类型(key和value)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(ProvincePartitioner.class);
        //如果小于ProvincePartitioner.getPartition的返回值，则会报错，大于的话，则多产生几个文件
        job.setNumReduceTasks(5);

        //设置程序的输入和输出参数，这里是程序需要统计文件所在的目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交yarn任务
//        job.submit();
        //等待yarn的任务执行完成
        boolean res = job.waitForCompletion(true);
        //根据程序的执行状态返回一个当前程序的执行状态
        System.exit(res?0:1);
    }
}
