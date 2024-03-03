package com.blue.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 10:10 2018/1/10
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

public class HDFSTestCase {
    private FileSystem fs = null;
    private Configuration conf = null;

    @Before
    public void init() throws Exception {
        conf = new Configuration();
//        conf.set("hadoop.home.dir", "D:/develop/hadoop-3.0.0");
//        conf.set("fs.default.name","hdfs://192.168.163.111:9000");
        fs = FileSystem.get(new URI("hdfs://192.168.163.111:9000"),conf,"hadoop");
    }

    /**
     * 上传文件
     * @throws IOException
     */
    @Test
    public void testUploadFile() throws IOException {
        fs.copyFromLocalFile(new Path("D:/software/jdk-8u151-linux-x64.tar.gz"),new Path("/software/jdk-8u151-linux-x64.tar.gz.copy"));
        fs.close();
        System.out.println("upload success!!");
    }

    /**
     * 下载文件
     * @throws IOException
     */
    @Test
    public void testDownloadFile() throws IOException {
        fs.copyToLocalFile(false,new Path("/software/jdk-8u151-linux-x64.tar.gz"),new Path("D:/software/jdk-8u151-linux-x64.tar.gz.copy"),false);
        fs.close();
        System.out.println("download success!!");
    }

    /**
     * 查看conf的默认参数
     */
    @Test
    public void testConf(){
        Iterator<Map.Entry<String,String>> iterator = conf.iterator();
        while(iterator.hasNext()){
            Map.Entry<String,String> entry = iterator.next();
            System.out.println("key:"+entry.getKey()+"\t"+"value:"+entry.getValue());
        }
    }
}
