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
 * @Description 找到两两账号的共同好友，其中key是两个账号的映射，value是多个共同的好友
 * @Date: Create in 10:20 2018/1/23
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

public class SharedFriendsStepTwo {
    static class SharedFriendsStepTwoMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] persons = value.toString().split("\\s");
            String friend = persons[0];
            String[] persons_persons = persons[1].split(",");

            Arrays.sort(persons_persons);

            for(int i = 0 ; i<persons_persons.length-1;i++){
                for(int j=i+1;j<persons_persons.length;j++){
                    context.write(new Text(persons_persons[i]+"--"+persons_persons[j]),new Text(friend));
                }
            }
        }
    }

        static class SharedFriendsStepTwoReducer extends Reducer<Text,Text,Text,Text> {
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

            job.setJarByClass(SharedFriendsStepTwo.class);

            job.setMapperClass(SharedFriendsStepTwoMapper.class);
            job.setReducerClass(SharedFriendsStepTwoReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job,new Path("D:/tmp/data/shared-friends/output-step1"));
            FileOutputFormat.setOutputPath(job,new Path("D:/tmp/data/shared-friends/output-step2"));

            boolean res = job.waitForCompletion(true);

            System.exit(res?0:1);
        }
    }
