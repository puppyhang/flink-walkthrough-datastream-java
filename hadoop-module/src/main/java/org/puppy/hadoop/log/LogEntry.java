package org.puppy.hadoop.log;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 日志类型,实现Hadoop规范
 *
 * @author puppy(陶江航 taojianghangdsjb @ smegz.cn)
 * @since 2021/3/8 17:01
 */
public class LogEntry implements Writable {

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
