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
 * @Date: create in 2019/2/2 10:53
 * @Modified by:
 * @Project: learning-hbase
 * @Package: com.blue.hbase.test.syntax.example
 * @Description:
 */
public class CounterTestCase {

    Logger logger = LoggerFactory.getLogger(getClass());

    Configuration conf = null;
    Connection conn = null;
    HBaseAdmin admin = null;

    static String tableName = "counters";
    static String rowkey_20190202 = "20190202";
    static String rowkey_20110101 = "20110101";
    static String family_daily = "daily";
    static String family_weekly = "weekly";
    static String family_monthly = "monthly";
    static String family_yearly = "yearly";
    static String column_hits = "hits";
    static String column_clicks = "clicks";

    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(conf);
        admin = (HBaseAdmin) conn.getAdmin();
    }

    @Test
    public void singleCounter() throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        table.incrementColumnValue(Bytes.toBytes(rowkey_20190202), Bytes.toBytes(family_daily), Bytes.toBytes(column_hits), 1);
        table.incrementColumnValue(Bytes.toBytes(rowkey_20110101), Bytes.toBytes(family_daily), Bytes.toBytes(column_hits), -2);
        table.close();

        logger.info("自增成功");
    }

    @Test
    public void incrementCounter() throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        Increment increment = new Increment(Bytes.toBytes(rowkey_20190202));
        increment.addColumn(Bytes.toBytes(family_daily), Bytes.toBytes(column_hits), -2);
        increment.addColumn(Bytes.toBytes(family_daily), Bytes.toBytes(column_clicks), 5);
        increment.addColumn(Bytes.toBytes(family_weekly), Bytes.toBytes(column_hits), 3);
        increment.addColumn(Bytes.toBytes(family_weekly), Bytes.toBytes(column_clicks), -2);
        table.increment(increment);

        table.close();

        logger.info("自增成功");
    }

    @After
    public void destroy() throws IOException {
        admin.close();
        conn.close();
    }

}
