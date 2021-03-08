package org.puppy.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 词频统计的mapper
 *
 * @author puppy(陶江航 taojianghangdsjb @ smegz.cn)
 * @since 2021/3/8 14:50
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static Logger LOGGER = LoggerFactory.getLogger(WordCountMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {

        String[] words = value.toString().split(" ");

        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }

        LOGGER.info("执行map完毕!");
    }
}
