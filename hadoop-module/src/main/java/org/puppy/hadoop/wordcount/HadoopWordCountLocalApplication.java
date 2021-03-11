package org.puppy.hadoop.wordcount;

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
public class HadoopWordCountLocalApplication {
    private final static Logger LOGGER = LoggerFactory.getLogger(HadoopWordCountLocalApplication.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {


        JobConf jobconf = new JobConf();

        Job job = Job.getInstance(jobconf);
        job.setJarByClass(MaxTemperatureRunner.class);
        job.setJobName("WordCount");
        FileInputFormat.addInputPath((JobConf) job.getConfiguration(), new Path(args[0]));
        FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(args[1]));

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean result = job.waitForCompletion(true);

        LOGGER.info("词频统计" + (result ? "成功" : "失败"));
    }
}
