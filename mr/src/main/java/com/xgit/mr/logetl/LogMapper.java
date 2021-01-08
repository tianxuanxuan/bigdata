package com.xgit.mr.logetl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xgxx tianxx
 * @date 2021-01-08 15:32:02
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        boolean result = parseLog(line, context);

        //不合法数据，退出
        if (!result){
            return;
        }

        //设置key
        k.set(line);

        context.write(k, NullWritable.get());
    }

    private boolean parseLog(String line, Context context){
        String[] fields = line.split(" ");

        if (fields.length > 11){
            context.getCounter("map", "true").increment(1);
            return true;
        }else {
            context.getCounter("map", "false").increment(1);
            return false;
        }
    }
}
