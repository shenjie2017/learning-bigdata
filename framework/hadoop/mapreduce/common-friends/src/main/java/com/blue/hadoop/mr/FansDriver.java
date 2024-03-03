package com.blue.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * @author shenjie
 * @version v1.0
 * @Description 找出互粉的共同好友
 * @Date: Create in 11:08 2018/1/23
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

public class FansDriver {

    private static class FansMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] persons = value.toString().split(":");
            String person = persons[0];
            String[] fans = persons[1].split(",");

            for(String fan:fans){
                context.write(new Text(buildKey(person,fan)),new Text(person));
            }
        }

        private String buildKey(String s1,String s2){
            if(s1.compareTo(s2)>0){
                return s2 + "-" + s1;
            }else {
                return s1 + "-" + s2;
            }
        }
    }

    private static class FansReducer extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //有两个以上的好友关系，则说明这两个互为粉丝(数据源不能有重复数据)

            int count = 0;
            for(Text value:values){
                count++;
            }
            if(count>=2){
                context.write(key,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(FansDriver.class);

        job.setMapperClass(FansMapper.class);
        job.setReducerClass(FansReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("D:/tmp/data/common-friends/input"));
        FileOutputFormat.setOutputPath(job,new Path("D:/tmp/data/common-friends/output"));

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);
    }
}
