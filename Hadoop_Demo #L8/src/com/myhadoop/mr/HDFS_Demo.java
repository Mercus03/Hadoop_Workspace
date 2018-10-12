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

/**
 * @author 84116
 *
 */
public class HDFS_Demo {
    public static void main(String[] args)throws Exception{
        FileSystem fs = FileSystem.get(new URI("hdfs://mercus01:9000"),new Configuration());
        InputStream in = fs.open(new Path("/jdk1.7"));
        OutputStream out = new FileOutputStream("D://Hadoop/jdk1.7");
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
