package com.iteblog;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
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
public class Entry implements WritableComparable<Entry> {
    private String yearMonth;
    private int count;

    public Entry() {
    }

    public Entry(String yearMonth, int count) {
        this.yearMonth = yearMonth;
        this.count = count;
    }

    @Override
    public int compareTo(Entry entry) {
        int result = this.yearMonth.compareTo(entry.getYearMonth());
        if (result == 0) {
            result = compare(count, entry.getCount());
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(yearMonth);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.yearMonth = dataInput.readUTF();
        this.count = dataInput.readInt();
    }

    public String getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(String yearMonth) {
        this.yearMonth = yearMonth;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public static int compare(int a, int b) {
        return a < b ? -1 : (a > b ? 1 : 0);
    }

    @Override
    public String toString() {
        return yearMonth;
    }
}
