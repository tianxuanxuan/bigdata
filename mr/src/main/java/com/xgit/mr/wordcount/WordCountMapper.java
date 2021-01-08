package com.xgit.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xgxx tianxx
 * @date 2021-01-07 14:45:15
 */
//map阶段
//四个参数分别为输入数据额key，输入数据的value，输出数据的key类型（atguigu，1 ss,1），数据数据的value类型
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();

        //2.切割单词
        String[] words = line.split(" ");

        //3.循环写出
        for (String word : words){
            k.set(word);
            context.write(k, v);
        }
    }
}
