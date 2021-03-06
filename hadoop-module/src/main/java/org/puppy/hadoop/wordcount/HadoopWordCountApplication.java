package org.puppy.hadoop.wordcount;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.puppy.hadoop.maxtemperature.MaxTemperatureRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 只利用hadoop实现word count程序
 *
 * @author puppy(陶江航 taojianghangdsjb @ smegz.cn)
 * @since 2021/3/5 16:38
 */
@SuppressWarnings("DuplicatedCode")
public class HadoopWordCountApplication {
    private final static Logger LOGGER = LoggerFactory.getLogger(HadoopWordCountApplication.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {

        System.setProperty("HADOOP_USER_NAME", "puppy");

        JobConf jobconf = new JobConf();
        jobconf.set("fs.defaultFS", "hdfs://hadoop000:9000");

        Job job = Job.getInstance(jobconf);
        job.setJarByClass(MaxTemperatureRunner.class);
        job.setJobName("WordCount");


        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        /*在数据发送到reducer之前 先对单个mapper执行reduce操作，合并部分结果再进行网络传输,
        较少io，提升作业的执行性能，但是并非所有作业都符合这个特性，参考《Hadoop权威指南》*/
        job.setCombinerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path(args[1]);
        //如果输出目录已经存在则先删除目录
        FileSystem fs = FileSystem.get(jobconf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath((JobConf) job.getConfiguration(), new Path(args[0]));
        FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), outputPath);

        boolean result = job.waitForCompletion(true);

        LOGGER.info("词频统计" + (result ? "成功" : "失败"));
    }
}
