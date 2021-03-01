package org.puppy.hadoop.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author puppy(陶江航 taojianghangdsjb @ smegz.cn)
 * @since 2021/3/1 11:24
 */
public class MaxTemperatureMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
