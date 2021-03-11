package org.puppy.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * map的输出，相同的key分配到一个reducer上
 *
 * @author puppy(陶江航 taojianghangdsjb @ smegz.cn)
 * @since 2021/3/8 14:50
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final static Logger LOGGER = LoggerFactory.getLogger(WordCountReducer.class);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        Iterator<IntWritable> valuesIterator = values.iterator();

        int counter = 0;
        while (valuesIterator.hasNext()) {
            IntWritable numberOfTimes = valuesIterator.next();
            counter += numberOfTimes.get();
        }
        context.write(key, new IntWritable(counter));

        LOGGER.info("执行reduce完毕！");
    }
}
