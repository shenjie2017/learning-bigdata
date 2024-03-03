package com.blue.hadoop.mr;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author shenjie
 * @version v1.0
 * @Description 将订单表和产品表合并成订单信息
 * @Date: Create in 14:17 2018/1/19
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

public class OrderMergeDriver {

    static class OrderMergeMapper extends Mapper<LongWritable,Text,Text,OrderBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            //空白符(空格制表符)分隔各属性
            String[] fields = value.toString().split("\\s+");
            OrderBean order = new OrderBean();
            Text pid = new Text();

            if(filename.startsWith("order")){
                pid.set(fields[2]);
                order.setType(OrderBean.TYPE_ORDER);

                order.setId(fields[0]);
                order.setDate(fields[1]);
                order.setPid(fields[2]);
                order.setNum(Integer.valueOf(fields[3]));
                order.setPlatform(fields[4]);
            }else if(filename.startsWith("product")){
                pid.set(fields[0]);
                order.setType(OrderBean.TYPE_PRODUCT);
                order.setPid(fields[0]);
                order.setPdName(fields[1]);
                order.setPdColor(fields[2]);
                order.setPrice(Double.valueOf(fields[3]));
                order.setConfigParams(fields[4]);
            }

            context.write(pid,order);
        }
    }

    static class OrderMergeReducer extends Reducer<Text,OrderBean,OrderBean,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
            List<OrderBean> list = new ArrayList<OrderBean>();
            OrderBean product = new OrderBean();
            for(OrderBean bean:values){
                if(OrderBean.TYPE_ORDER == bean.getType() ){
                    try {
                        OrderBean order = new OrderBean();
                        BeanUtils.copyProperties(order,bean);
                        list.add(order);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }else if(OrderBean.TYPE_PRODUCT == bean.getType()){
                    try {
                        BeanUtils.copyProperties(product,bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            for(OrderBean bean:list){
                //在订单中填充产品信息
                bean.setPid(product.getPid());
                bean.setPdName(product.getPdName());
                bean.setPdColor(product.getPdColor());
                bean.setPrice(product.getPrice());
                bean.setConfigParams(product.getConfigParams());

                context.write(bean,NullWritable.get());
            }
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
        job.setJarByClass(OrderMergeDriver.class);

        //设置map task处理类
        job.setMapperClass(OrderMergeMapper.class);
        //设置reduce task处理类
        job.setReducerClass(OrderMergeReducer.class);

        //设置map task的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);

        //设置reduce task的输出类型(key和value)
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

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
