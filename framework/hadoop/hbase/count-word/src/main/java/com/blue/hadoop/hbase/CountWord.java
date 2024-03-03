package com.blue.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 16:48 2018/5/3
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

public class CountWord {

    public final static String FROMCOLF = "extend";
    public final static String FROMCOL = "desc";

    public final static String TOCOLF = "base";
    public final static String TOCOL = "count";

    public static class MyMapper extends TableMapper<Text, IntWritable> {

        private final IntWritable ONE = new IntWritable(1);
        private Text text = new Text();

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            System.out.println("row:"+row.toString());
            String[] words = Bytes.toString(value.getValue(Bytes.toBytes(FROMCOLF),Bytes.toBytes(FROMCOL))).split(" ");
            for(String word:words){
                text.set(word);
                context.write(text, ONE);
            }

        }
    }

    public static class MyReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes(TOCOLF), Bytes.toBytes(TOCOL), Bytes.toBytes(String.valueOf(sum)));

            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String fromTableName = "content";
        String toTableName = "wordcount";

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.163.111");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(toTableName)){
            System.out.println("table exists!recreating.......");
            admin.disableTable(toTableName);
            admin.deleteTable(toTableName);
        }
        HTableDescriptor htd = new HTableDescriptor(toTableName);
        HColumnDescriptor tcd = new HColumnDescriptor(TOCOLF);
        htd.addFamily(tcd);
        admin.createTable(htd);

        Scan scan = new Scan();


        Job job = new Job(conf, "WordCountHbase");
        job.setJarByClass(CountWord.class);
        //使用WordCountHbaseMapper类完成Map过程；
        job.setMapperClass(MyMapper.class);
        TableMapReduceUtil.initTableMapperJob(fromTableName,scan,MyMapper.class,Text.class,IntWritable.class,job);
        TableMapReduceUtil.initTableReducerJob(toTableName, MyReducer.class, job);
        //设置了Map过程和Reduce过程的输出类型，其中设置key的输出类型为Text；
//        job.setOutputKeyClass(Text.class);
        //设置了Map过程和Reduce过程的输出类型，其中设置value的输出类型为IntWritable；
//        job.setOutputValueClass(IntWritable.class);
        //调用job.waitForCompletion(true) 执行任务，执行成功后退出；
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
