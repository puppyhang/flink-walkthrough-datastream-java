package org.puppy.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;

/**
 * 词频统计的mapper
 * <p>
 * Mapper接口类型参数解释
 * KEYIN 偏移量
 * VALUEIN Hadoop split 数据之后的数据(通常是一行)
 *
 * @author puppy(陶江航 taojianghangdsjb @ smegz.cn)
 * @since 2021/3/8 14:50
 * <p>
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static Logger LOGGER = LoggerFactory.getLogger(WordCountMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {

        String[] words = value.toString().split(" ");

        for (String word : words) {
            context.write(new Text(word.toLowerCase(Locale.ROOT)), new IntWritable(1));
        }

        LOGGER.info("执行map完毕!");
    }
}
