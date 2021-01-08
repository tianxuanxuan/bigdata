package com.xgit.mr.logetl2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xgxx tianxx
 * @date 2021-01-08 17:15:35
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取1行
        String line = value.toString();

        // 2 解析日志是否合法
        LogBean bean = parseLog(line);

        if (!bean.isValid()) {
            return;
        }

        k.set(bean.toString());

        // 3 输出
        context.write(k, NullWritable.get());

    }

    private LogBean parseLog(String line){
        LogBean logBean = new LogBean();
        // 截取
        String[] files = line.split(" ");
        if (files.length > 11){
            // 封装数据
            logBean.setRemote_addr(files[0]);
            logBean.setRemote_user(files[1]);
            logBean.setTime_local(files[3].substring(1));
            logBean.setRequest(files[6]);
            logBean.setStatus(files[8]);
            logBean.setBody_bytes_sent(files[9]);
            logBean.setHttp_referer(files[10]);

            if (files.length > 12){
                logBean.setHttp_user_agent(files[11] + " " + files[12]);
            }else {
                logBean.setHttp_user_agent(files[11]);
            }

            //大于400，HTTP错误
            if (Integer.parseInt(logBean.getStatus()) >= 400){
                logBean.setValid(false);
            }
        }else {
            logBean.setValid(false);
        }
        return logBean;
    }
}
