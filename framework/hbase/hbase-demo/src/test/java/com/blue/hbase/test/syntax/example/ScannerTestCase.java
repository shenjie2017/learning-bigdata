package com.blue.hbase.test.syntax.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Author: Jason
 * @E-mail: 1075850619@qq.com
 * @Date: create in 2019/2/1 10:10
 * @Modified by:
 * @Project: learning-hbase
 * @Package: com.blue.hbase.test.syntax.demo
 * @Description:
 */
public class ScannerTestCase {
    Logger logger = LoggerFactory.getLogger(getClass());

    Configuration conf = null;
    Connection conn = null;
    HBaseAdmin admin = null;

    static String tableName = "student";
    static String rowkey = "stu100001";
    static String family = "info";

    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(conf);
        admin = (HBaseAdmin) conn.getAdmin();
    }

    @Test
    public void scannerTable() throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("stu10000"));
        scan.withStopRow(Bytes.toBytes("stu10001"));
        scan.addFamily(Bytes.toBytes(family));
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes("name")).addColumn(Bytes.toBytes(family), Bytes.toBytes("sex"));
        //缓存100行数据
        scan.setCaching(100);
        //缓存一行的50列数据
        scan.setBatch(50);
        ResultScanner resultScanner = null;
        try {
            resultScanner = table.getScanner(scan);
            Result result = null;
            while (null != (result = resultScanner.next())) {
                logger.info(result.toString());
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            resultScanner.close();
        }
        table.close();
    }

    @After
    public void destroy() throws IOException {
        admin.close();
        conn.close();
    }

}
