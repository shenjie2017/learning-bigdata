package com.blue.hadoop.mr;

import com.blue.hadoop.mr.driver.MaxTemperatureDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author: Jason
 * @E-mail: 1075850619@qq.com
 * @Date: create in 2018/9/20 16:29
 * @Modified by:
 * @Project: learning-hadoop
 * @Package: com.blue.hadoop.mr
 * @Description:
 */
public class TestCase {
    @Test
    public void testRunMR() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.framework.name","local");
        conf.setInt("mapreduce.task.io.sort.mb",1);

        Path in = new Path( "D:/tmp/hdfs/test/in/temperature/cd_temperature.dat");
        Path out = new Path("D:/tmp/hdfs/test/out/temperature");

        FileSystem fs = FileSystem.getLocal(conf);
        //delete old output folder
        fs.delete(out,true);

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);
        int exitCode = driver.run(new String[]{in.toString(),out.toString()});

        assert exitCode==0;


    }
}
