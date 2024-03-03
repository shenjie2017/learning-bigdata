package com.blue.hbase.test.syntax.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
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
 * @Date: create in 2019/2/1 10:09
 * @Modified by:
 * @Project: learning-hbase
 * @Package: com.blue.hbase.test.syntax.demo
 * @Description:
 */
public class FilterTestCase {

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
    public void rowFilter() throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = null;
        ResultScanner resultScanner = null;
        try {
            scan = new Scan();
            Result result = null;
            Filter filter = new RowFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("stu10001")));
            resultScanner = table.getScanner(scan);
            scan.setFilter(filter);
            while (null != (result = resultScanner.next())) {
                logger.info("BinaryComparator:" + result.toString());
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            resultScanner.close();
        }

        try {
            scan = new Scan();

            Result result = null;
            Filter filter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("1$"));
            scan.setFilter(filter);
            resultScanner = table.getScanner(scan);
            while (null != (result = resultScanner.next())) {
                logger.info("RegexStringComparator:" + result.toString());
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            resultScanner.close();
        }

        try {
            scan = new Scan();
            Result result = null;
            Filter filter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator("0001"));
            scan.setFilter(filter);
            resultScanner = table.getScanner(scan);
            while (null != (result = resultScanner.next())) {
                logger.info("SubstringComparator:" + result.toString());
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            resultScanner.close();
        }

        table.close();
    }

    @Test
    public void famliyFilter() throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = null;
        ResultScanner resultScanner = null;
        Filter filter = null;
        Result result = null;
        Get get = null;

        try {
            scan = new Scan();
            filter = new FamilyFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("info")));
            scan.setFilter(filter);
            resultScanner = table.getScanner(scan);
            while (null != (result = resultScanner.next())) {
                logger.info("FamilyFilter BinaryComparator:" + result.toString());
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            resultScanner.close();
        }

        get = new Get(Bytes.toBytes("stu100003"));
        get.setFilter(filter);
        result = table.get(get);
        logger.info("get " + ":" + result.toString());

        try {
            scan = new Scan();
            filter = new FamilyFilter(CompareOperator.LESS, new BinaryComparator(Bytes.toBytes("info")));
            scan.setFilter(filter);
            resultScanner = table.getScanner(scan);
            while (null != (result = resultScanner.next())) {
                logger.info("FamilyFilter BinaryComparator:" + result.toString());
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            resultScanner.close();
        }

        get = new Get(Bytes.toBytes("stu100003"));
        get.setFilter(filter);
        result = table.get(get);
        logger.info("get stu100003" + ":" + result.toString());
    }

    @Test
    public void qualifierFilter() throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = null;
        Filter filter = null;
        Result result = null;
        try {
            filter = new QualifierFilter(CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("name")));
            scan.setFilter(filter);
            resultScanner = table.getScanner(scan);

            while (null != (result = resultScanner.next())) {
                logger.info("QualifierFilter BinaryComparator:" + result.toString());
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            resultScanner.close();
        }

        Get get = new Get(Bytes.toBytes(rowkey));
        get.setFilter(filter);
        result = table.get(get);
        logger.info("get " + rowkey + ":" + result.toString());
    }

    @Test
    public void valueFilter() throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = null;
        Result result = null;
        Filter filter = new ValueFilter(CompareOperator.EQUAL, new RegexStringComparator("i"));
        scan.setFilter(filter);

        try {
            resultScanner = table.getScanner(scan);

            while (null != (result = resultScanner.next())) {
                for (Cell cell : result.listCells()) {
                    logger.info("rowkey:" + Bytes.toString(result.getRow()) +
                            "\tfamliy:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) +
                            "\tcolumn:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) +
                            "\tvalue:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            resultScanner.close();
        }

        Get get = new Get(Bytes.toBytes(rowkey));
        get.setFilter(filter);
        result = table.get(get);
        logger.info("get " + rowkey + ":" + result.toString());
    }

    @Test
    public void singleColumnValueFilter() throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = null;
        Result result = null;
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes("name"), CompareOperator.EQUAL, new RegexStringComparator("i"));
        scan.setFilter(filter);
//        ((SingleColumnValueFilter) filter).setFilterIfMissing(true);
        try {
            resultScanner = table.getScanner(scan);

            while (null != (result = resultScanner.next())) {
                for (Cell cell : result.listCells()) {
                    logger.info("rowkey:" + Bytes.toString(result.getRow()) +
                            "\tfamliy:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) +
                            "\tcolumn:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) +
                            "\tvalue:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            resultScanner.close();
        }

        Get get = new Get(Bytes.toBytes(rowkey));
        get.setFilter(filter);
        result = table.get(get);
        logger.info("get " + rowkey + ":" + result.toString());
    }

    @After
    public void destroy() throws IOException {
        admin.close();
        conn.close();
    }

}
