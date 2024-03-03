package com.blue.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 16:58 2018/1/12
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

public class HDFSStreamTestCase {
    private FileSystem fs = null;
    private Configuration conf = null;

    @Before
    public void init() throws Exception {
        conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://192.168.163.111:9000"),conf,"hadoop");
    }

    /**
     * 使用流上传
     */
    @Test
    public void testUpload() throws IOException {
        FSDataOutputStream out = fs.create(new Path("/software/apache-tomcat-7.0.82.tar.gz"));
        FileInputStream in = new FileInputStream("d:/software/apache-tomcat-7.0.82.tar.gz");
        IOUtils.copy(in,out);
        System.out.println("流上传成功");
    }

    /**
     * 流下载
     */
    @Test
    public void testDownload() throws IOException {
        FSDataInputStream in = fs.open(new Path("/software/apache-tomcat-7.0.82.tar.gz"));
        FileOutputStream out = new FileOutputStream("d:/tmp/tomcat.tar.gz");
        IOUtils.copy(in,out);
        System.out.println("流下载完成");
    }

    /**
     * 下载部分文件
     * @throws IOException
     */
    @Test
    public void testCopyBlock() throws IOException {
        FSDataInputStream in = fs.open(new Path("/tmpdir/hello.log"));
        in.seek(10);
        FileOutputStream out = new FileOutputStream("d:/tmp/hello.log");
        IOUtils.copy(in,out);
        System.out.println("下载部分文件完成");

    }

    /**
     * 查看文件的详情
     * @throws IOException
     */
    @Test
    public void testListFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"),true);
        while(iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();
            System.out.println("filename:"+fileStatus.getPath().getName());
            System.out.println("owner:"+fileStatus.getOwner());
            System.out.println("permission:"+fileStatus.getPermission());
            System.out.println("blockLocations:");
            BlockLocation[] locations = fileStatus.getBlockLocations();
            for(BlockLocation block:locations){
                System.out.println("\thosts:"+ Arrays.deepToString(block.getHosts()));
                System.out.println("\tname:"+Arrays.deepToString(block.getNames()));
                System.out.println("\tstartoffset:"+block.getOffset());
                System.out.println("\tlength:"+block.getLength());
            }
            System.out.println("----------------------------");
        }
    }



}
