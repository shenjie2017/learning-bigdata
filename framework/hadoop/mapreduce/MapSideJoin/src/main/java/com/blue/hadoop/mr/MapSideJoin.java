package com.blue.hadoop.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 15:08 2018/1/22
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

public class MapSideJoin {
    static class MapSideJoinMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        private Map<String,String> pdInfoMap = new HashMap<String, String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("product-mobile.dat")));
            String line = null;
            while(StringUtils.isNotEmpty(line=br.readLine())){
                String[] fields = line.split("\\s+");
                pdInfoMap.put(fields[0],fields[1]);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");
            String pdInfo = pdInfoMap.get(fields[2]);
            context.write(new Text(value.toString()+"\t"+pdInfo),NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.map.memory.mb","2048");
        conf.set("mapreduce.reduce.memory.mb","2048");
        conf.set("mapreduce.map.java.opts","-Xmx2048m");
        conf.set("mapreduce.reduce.java.opts","-Xmx2048m");
        //构造yarn的任务
        Job job = Job.getInstance(conf);

        //设置jar包的位置
        job.setJarByClass(MapSideJoin.class);

        //设置map task处理类
        job.setMapperClass(MapSideJoinMapper.class);

        //设置map task的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

//        job.addCacheFile(new URI("file:///home/hadoop/product-mobile.dat"));
        job.addCacheFile(new URI("file:///home/hadoop/product-mobile.dat"));

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
