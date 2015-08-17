package com.iteblog;


import org.apache.hadoop.mapreduce.Partitioner;

/**
 * User: 过往记忆
 * Date: 2015-08-05
 * Time: 下午23:49
 * bolg: http://www.iteblog.com
 * 本文地址：http://www.iteblog.com/archives/1415
 * 过往记忆博客，专注于hadoop、hive、spark、shark、flume的技术博客，大量的干货
 * 过往记忆博客微信公共帐号：iteblog_hadoop
 */
public class EntryPartitioner extends Partitioner<Entry, Integer> {

    @Override
    public int getPartition(Entry entry, Integer integer, int numberPartitions) {
        return Math.abs((int) (hash(entry.getYearMonth()) % numberPartitions));
    }

    static long hash(String str) {
        long h = 1125899906842597L; // prime
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31 * h + str.charAt(i);
        }
        return h;
    }
}
