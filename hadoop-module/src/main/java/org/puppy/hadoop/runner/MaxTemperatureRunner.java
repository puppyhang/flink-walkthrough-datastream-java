package org.puppy.hadoop.runner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.puppy.hadoop.map.MaxTemperatureMapper;
import org.puppy.hadoop.reduce.MaxTemperatureReducer;

import javax.xml.soap.Text;
import java.io.IOException;

/**
 * map reduce 调度程序，打包为jar，部署到hadoop中执行
 *
 * @author puppy(陶江航 taojianghangdsjb @ smegz.cn)
 * @since 2021/3/1 11:35
 */
public class MaxTemperatureRunner {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(MaxTemperatureRunner.class);
        job.setJobName("CalcMaxTemperature");
        FileInputFormat.addInputPath(new JobConf(job.getConfiguration()), new Path(args[0]));
        FileOutputFormat.setOutputPath(new JobConf(job.getConfiguration()), new Path(args[0]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}
