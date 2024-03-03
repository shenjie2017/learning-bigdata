package com.blue.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 16:30 2018/1/24
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

public class LogEnhanceDriver {

    static class LogEnhanceMapper extends Mapper<LongWritable,Text,Text,NullWritable>{

        private Map<String,String> urlMap = null;

        /**
         * 模拟从数据读取数据
         * @return
         */
        private Map<String,String> readDB(){
            Map<String,String> urlMap = new HashMap<String, String>();
            urlMap.put("http://www.baidu.com","百度一下，你就知道");
            urlMap.put("http://www.google.com","谷歌大发好");
            urlMap.put("http://www.tencent.com","腾讯帝国万岁");
            urlMap.put("http://www.github.com","最爱开源");
            return urlMap;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            /**
             * 从数据库读取数据
             */
            urlMap = readDB();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /*
            这里将数据分类:
            以前访问过，则对该网站热点增加，将该信息写入热点文件
            如果是首次访问，则存入爬虫文件，并用其他程序将该网页爬去下来
             */

            //malformd是计数器的组名，malformdline是计数器名，这里计算解析出错的行数
            Counter counter = context.getCounter("malformd","malformdline");
            try{
                String[] params = value.toString().split("\\s+");
                String url = params[2];
                String date = params[1];
                String user = params[0];
                String plateform = params[3];
                String content = params[4];

                if(url.contains("http://")){
                    if(urlMap.containsKey(url)){
                        //曾经访问过这个网址，加入热点增强
                        context.write(new Text(url+"\t"+plateform+"\t"+date+"\t"+user+"\t"+content+"\n"),NullWritable.get());
                    }else{
                        //未访问过该网址，将该网址加入爬虫爬取文件
                        context.write(new Text(url+"\t"+plateform+"\t"+date+"\t"+user+"\t"+"tocrawl"+"\n"),NullWritable.get());
                    }
                }
            }catch (Exception e){
                //自增1
                counter.increment(1);
            }
        }
    }

    static class LogFileOutputFormat extends FileOutputFormat<Text,NullWritable>{

        @Override
        public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataOutputStream enhanceOS = fs.create(new Path("D:/tmp/data/log-enhance/enhance/access.dat"));
            FSDataOutputStream crawlOS = fs.create(new Path("D:/tmp/data/log-enhance/crawl/access.dat"));

            return new LogRecordWriter(enhanceOS,crawlOS);
        }
    }

    static class LogRecordWriter extends RecordWriter<Text,NullWritable>{

        private FSDataOutputStream enhanceOS = null;
        private FSDataOutputStream crawlOS = null;

        public LogRecordWriter(FSDataOutputStream enhanceOS, FSDataOutputStream crawlOS) {
            this.enhanceOS = enhanceOS;
            this.crawlOS = crawlOS;
        }

        @Override
        public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
            String[] params = text.toString().split("\\s+");
            String record = text.toString();
            if(record.contains("tocrawl")){
                crawlOS.write(record.getBytes());
            }else{
                enhanceOS.write(record.getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if(null!=enhanceOS){
                enhanceOS.close();
                enhanceOS = null;
            }
            if(null!=crawlOS){
                crawlOS.close();
                crawlOS = null;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(LogEnhanceDriver.class);

        job.setMapperClass(LogEnhanceMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(LogFileOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);
    }
}
