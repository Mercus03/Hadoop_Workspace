/**
 * 84116
 *2018年9月28日
 */
package com.myhadoop.mr;

import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * @author 84116
 *
 */
public class HDFS_Demo2 {
    static FileSystem fs = null;
    
    @Before
    public void init() throws Exception{
        fs = FileSystem.get(new URI("hdfs://mercus01:9000"),new Configuration(),"root");
    }
    
    @Test
    public void testUpload() throws Exception{
        InputStream in = new FileInputStream("D://BaiduYunDownload//keygen.exe");
        OutputStream out = fs.create(new Path("/keygen.exe"));
        IOUtils.copyBytes(in, out, 4096, true);
    }
    
    @Test
    public void testDelete() throws Exception{
        Boolean flag = fs.delete(new Path("/mercus001"),true);
        System.out.println(flag);
    }
    
    @Test
    public void testMKdir() throws Exception{
        Boolean flag = fs.mkdirs(new Path("/mercus001"));
        System.out.println(flag);
    }
}
