package com.blue.hbase.test.syntax.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchGetCase {
    public static void main(String[] args) throws IOException {
//        Logger logger = LoggerFactory.getLogger(BatchGetCase.class);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop01:2181,hadoop02:2181,hadoop03:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        String tableName = "rs_goods_pipeline_basic_model";
        String family = "rs_goods_pipeline_basic_model";
        Table table = conn.getTable(TableName.valueOf(tableName));
        long start = System.currentTimeMillis();
        for(int i=1;i<=10000;i++) {
//            第1000次读取,耗时98589
//            第2000次读取,耗时199635
//            第3000次读取,耗时312410
//            Get get = new Get(Bytes.toBytes("ZLDJYS12-24V150W5V9_DE"));
//            get.addColumn(Bytes.toBytes(family), Bytes.toBytes("brandCode"));
//            Result result = table.get(get);
//            第1000次读取,耗时83636
//            第2000次读取,耗时167700
//            第3000次读取,耗时253488
            Result result = table.get(new Get(Bytes.toBytes("ZLDJYS12-24V150W5V9_DE")));
            if(i%1000==0){
                System.out.println("第"+i+"次读取,耗时"+(System.currentTimeMillis()-start));

            }
        }
        long useTime=System.currentTimeMillis()-start;
        System.out.println("use time: "+useTime);

        table.close();
        admin.close();
        conn.close();
    }
}

