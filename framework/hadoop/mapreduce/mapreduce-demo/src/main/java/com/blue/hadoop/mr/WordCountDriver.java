package com.blue.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 12:39 2018/1/16
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

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //生成默认配置
        Configuration conf = new Configuration();
//        conf.set("yarn.resourcemanager.hostname","192.168.163.111");
//        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.map.memory.mb","2048");
        conf.set("mapreduce.reduce.memory.mb","2048");
        conf.set("mapreduce.map.java.opts","-Xmx2048m");
        conf.set("mapreduce.reduce.java.opts","-Xmx2048m");
        //构造yarn的任务
        Job job = Job.getInstance(conf);

        //设置jar包的位置
        job.setJarByClass(WordCountDriver.class);

        //设置map task处理类
        job.setMapperClass(WordCountMapper.class);
        //设置reduce task处理类
        job.setReducerClass(WordCountReduce.class);

        //设置map task的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce task的输出类型(key和value)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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
