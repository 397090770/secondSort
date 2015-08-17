package com.iteblog;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yangping.wu on 2015-08-04.
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
