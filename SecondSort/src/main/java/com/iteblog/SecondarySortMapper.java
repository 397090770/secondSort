package com.iteblog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
public class SecondarySortMapper extends Mapper<LongWritable, Text, Entry, Text> {

    private Entry entry = new Entry();
    private Text value = new Text();

    @Override
    protected void map(LongWritable key, Text lines, Context context)
            throws IOException, InterruptedException {
        String line = lines.toString();
        String[] tokens = line.split(",");
        // YYYY = tokens[0]
        // MM = tokens[1]
        // count = tokens[2]
        String yearMonth = tokens[0] + "-" + tokens[1];
        int count = Integer.parseInt(tokens[2]);

        entry.setYearMonth(yearMonth);
        entry.setCount(count);
        value.set(tokens[2]);

        context.write(entry, value);
    }
}