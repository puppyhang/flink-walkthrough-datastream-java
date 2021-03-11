package org.puppy.hadoop.log;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.puppy.hadoop.maxtemperature.MaxTemperatureRunner;

import java.io.IOException;

/**
 * todo:something document for this class should be put here.
 *
 * @author puppy(陶江航 taojianghangdsjb @ smegz.cn)
 * @since 2021/3/9 10:31
 */
public class AccessLogStatisticApplication {

    public static void main(String[] args) throws IOException {
        JobConf jobconf = new JobConf();
        jobconf.set("fs.defaultFS", "hdfs://hadoop000:9000");

        Job job = Job.getInstance(jobconf);
        job.setJarByClass(MaxTemperatureRunner.class);
        job.setJobName("WordCount");

        job.setPartitionerClass(AccessLogPartitioner.class);
        //3个reduce task,默认通过hash取模计算
        job.setNumReduceTasks(3);
    }
}
