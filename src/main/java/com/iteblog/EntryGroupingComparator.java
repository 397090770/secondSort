package com.iteblog;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by yangping.wu on 2015-08-04.
 */
public class EntryGroupingComparator extends WritableComparator {
    public EntryGroupingComparator() {
        super(Entry.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Entry a1 = (Entry) a;
        Entry b1 = (Entry) b;
        return a1.getYearMonth().compareTo(b1.getYearMonth());
    }

//    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//        return compareBytes(b1, s1, l1, b2, s2, l2);
//    }
}