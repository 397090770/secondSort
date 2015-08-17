package com.iteblog;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: 过往记忆
 * Date: 2015-08-05
 * Time: 下午23:49
 * bolg: http://www.iteblog.com
 * 本文地址：http://www.iteblog.com/archives/1415
 * 过往记忆博客，专注于hadoop、hive、spark、shark、flume的技术博客，大量的干货
 * 过往记忆博客微信公共帐号：iteblog_hadoop
 */
public class SecondarySortReducer extends Reducer<Entry, Text, Entry, Text> {

    //private Text keyText = new Text();

    @Override
    protected void reduce(Entry key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (Text value : values) {
            builder.append(value.toString());
            builder.append(",");
        }
        //keyText.set(key.getYearMonth());
        context.write(key, new Text(builder.toString()));
    }
}
