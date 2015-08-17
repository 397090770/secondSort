package com.iteblog;


import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by yangping.wu on 2015-08-04.
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
