package com.hadoop.mr;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 84116
 *2018年9月29日
 */

/**
 * @author 84116
 *
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //我们已经自定义好了Mapper和RedUC而现在我们要做的就是把MapReduce作业提交上去
        //现在我们把MapReduce作业抽象成Job对象了
        Job job = Job.getInstance(new Configuration());
        
        //注意：一定要将main方法所在的类设置进来。
        job.setJarByClass(WordCount.class);
        //接下来我们设置一下Job的Mapper相关属性
        job.setMapperClass(WCMapper.class);//设置Mapper类
        job.setMapOutputKeyClass(Text.class);//设置K2的类型
        job.setMapOutputValueClass(LongWritable.class);//设置V2的类型
        //接下来我们得告诉程序我们应该去哪里读取文件。需要注意的是Path是指在Hadoop的HDFS系统上的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));//这里我们采用变量的形式传进来地址
        
        //接下来我们来设置一下Job的Reducer相关属性
        job.setReducerClass(WCReduce.class);//设置Reducer类
        job.setOutputKeyClass(Text.class);//设置K3的类型
        job.setOutputValueClass(LongWritable.class);//设置V3的类型
        //接下来我们得告诉程序应该把结果信息写到什么位置。注意：这里的Path依然是指文件在Hadoop的HDFS系统
        //上的路径。
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//我们依然采用变量的形式传进来输出地址。
        job.waitForCompletion(true);//把作业提交并且等待执行完成，参数为true的话，会打印进度和详情。
        }

}
