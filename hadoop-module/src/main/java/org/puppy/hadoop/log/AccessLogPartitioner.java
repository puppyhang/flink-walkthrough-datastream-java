package org.puppy.hadoop.log;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区
 *
 * @author puppy(陶江航 taojianghangdsjb @ smegz.cn)
 * @since 2021/3/9 10:29
 */
public class AccessLogPartitioner extends Partitioner<Text, LogEntry> {

    /**
     * 根据KEY计算数据传递到哪一个分区(reduce任务编号)
     */
    @Override
    public int getPartition(Text text, LogEntry logEntry, int numPartitions) {
        return 0;
    }
}
