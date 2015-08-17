package com.iteblog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * User: 过往记忆
 * Date: 2015-08-05
 * Time: 下午23:49
 * bolg: http://www.iteblog.com
 * 本文地址：http://www.iteblog.com/archives/1415
 * 过往记忆博客，专注于hadoop、hive、spark、shark、flume的技术博客，大量的干货
 * 过往记忆博客微信公共帐号：iteblog_hadoop
 */
public class Main {

    public static void main(String[] args) throws Exception {
        // Make sure there are exactly 2 parameters
        if (args.length != 2) {
            System.out.println("SecondarySort <input-dir> <output-dir>");
            throw new IllegalArgumentException("SecondarySort <input-dir> <output-dir>");
        }


        Configuration conf = new Configuration();
        conf.set("mapreduce.job.queuename", "datadev");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Main.class);
        job.setJobName("SecondarySort");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Entry.class);
        job.setOutputValueClass(Text.class);


        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setPartitionerClass(EntryPartitioner.class);
        job.setGroupingComparatorClass(EntryGroupingComparator.class);
        //job.setCombinerClass(SecondarySortReducer.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
