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
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Jason
 * @E-mail: 1075850619@qq.com
 * @Date: create in 2019/1/30 14:39
 * @Modified by:
 * @Project: learning-hbase
 * @Package: com.blue.hbase.test.syntax.demo
 * @Description:
 */

public class TableTestCase {
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
    public void createTable() throws IOException {
        if (admin.tableExists(TableName.valueOf(tableName))) {
            logger.info("表已存在");
        } else {
            TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build()).build();
            admin.createTable(tableDesc);
            logger.info("表创建成功");
        }
    }

    @Test
    public void deleteTable() throws IOException {
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        logger.info("表删除成功");
    }

    @Test
    public void listTable() throws IOException {
        List<TableDescriptor> list = admin.listTableDescriptors();
        for (TableDescriptor table : list) {
            logger.info("table name:" + table.getTableName() + "\ttable status:" + admin.isTableDisabled(table.getTableName()));
        }
    }

    @Test
    public void insertRecord() throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes("sex"), Bytes.toBytes("man"));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes("age"), Bytes.toBytes(65));
        table.put(put);
        table.close();
        logger.info("插入成功");
    }

    @Test
    public void deleteRecord() throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        delete.addColumn(Bytes.toBytes(family), Bytes.toBytes("sex"));
        table.delete(delete);
        table.close();
        logger.info("记录删除成功");
    }

    @Test
    public void selectRecord() throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        logger.info(result.toString());

        logger.info("rowkey: " + Bytes.toString(result.getRow()) + "\t name: " + Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("name"))));
        logger.info("rowkey: " + Bytes.toString(result.getRow()) + "\t sex: " + Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("sex"))));
        logger.info("rowkey: " + Bytes.toString(result.getRow()) + "\t age: " + Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("age"))));
        table.close();
    }

    @Test
    public void bitchRecord() throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        List<Row> batch = new ArrayList<Row>();

        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"));
        batch.add(get);

        Put put = new Put(Bytes.toBytes("stu100003"));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"), Bytes.toBytes("wangwu"));
        batch.add(put);

        Delete delete = new Delete(Bytes.toBytes("stu100002"));
        delete.addColumn(Bytes.toBytes(family), Bytes.toBytes("sex"));
        batch.add(delete);


        delete = new Delete(Bytes.toBytes("stu100002"));
        delete.addColumn(Bytes.toBytes(family), Bytes.toBytes("age"));
        batch.add(delete);

        get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes("no_family"), Bytes.toBytes("name"));
        batch.add(get);

        get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes("age"));
        batch.add(get);

        put = new Put(Bytes.toBytes("stu100003"));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes("age"), Bytes.toBytes(70));
        batch.add(put);

        Object[] results = new Object[7];

        try {
            table.batch(batch, results);
        } catch (Exception e) {
            logger.info(e.getMessage());
        }

        for (Object obj : results) {
            if (null != obj) {
                logger.info(obj.toString());
            }
        }
        table.close();
    }

    @After
    public void destroy() throws IOException {
        admin.close();
        conn.close();
    }

}
