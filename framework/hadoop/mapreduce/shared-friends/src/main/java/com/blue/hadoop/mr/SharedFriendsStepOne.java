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
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author shenjie
 * @version v1.0
 * @Description 找到某个好友是哪些账号的好友，其中key是具体某个好友，value是一系列账号
 * @Date: Create in 10:11 2018/1/23
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

public class SharedFriendsStepOne {
    static class SharedFriendsStepOneMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] persons = value.toString().split(":");
            String person = persons[0];
            String[] friends = persons[1].split(",");

            Arrays.sort(friends);

            for(int i = 0 ; i<friends.length;i++){
                context.write(new Text(friends[i]),new Text(person));
            }
        }
    }

    static class SharedFriendsStepOneReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for(Text value : values){
                sb.append(value.toString()).append(",");
            }
            context.write(key,new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(SharedFriendsStepOne.class);

        job.setMapperClass(SharedFriendsStepOneMapper.class);
        job.setReducerClass(SharedFriendsStepOneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("D:/tmp/data/shared-friends/input"));
        FileOutputFormat.setOutputPath(job,new Path("D:/tmp/data/shared-friends/output-step1"));

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);
    }
}
